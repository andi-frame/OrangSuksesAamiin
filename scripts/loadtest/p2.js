// Problem 2 load test: >= 50 parallel connections, 60 s sustained, against the Client-Facing API (:8080).
//
// Every VU authenticates like a real Tim Lapangan client: it logs in once with its client credentials and
// then renews its short-lived access token through POST /v1/refresh shortly BEFORE it expires, so the run is
// not limited by the 60 s access TTL (an earlier version reused one token and measured its own expiry as errors).
//
// Classification (spec): 429 is a controlled rejection -> reported, not an error.
// "unhandled" = anything that is neither 200 nor 429 (5xx, 401/403, timeouts) and must stay below 1 %.
import http from 'k6/http';
import { sleep } from 'k6';
import { Counter, Rate } from 'k6/metrics';

const BASE = __ENV.CLIENT_API_URL || 'http://localhost:8080';
const AUTH = __ENV.AUTH_URL || 'http://localhost:8084';
const CLIENT_ID = __ENV.K6_CLIENT_ID;
const CLIENT_SECRET = __ENV.K6_CLIENT_SECRET;
const PATH = '/v1/hazards?hazard_type=SEISMIC&limit=20';
const VUS = parseInt(__ENV.K6_VUS || '50');
const DURATION = __ENV.K6_DURATION || '60s';
const THINK_S = parseFloat(__ENV.K6_THINK_S || '0.1');
const RENEW_MARGIN_MS = 10000;
const SUMMARY = __ENV.K6_SUMMARY || 'docs/evidence/p2/k6-summary.json';

const ok200 = new Counter('resp_200');
const shed429 = new Counter('resp_429');
const other = new Counter('resp_other');
const unhandled = new Rate('unhandled');
const renewals = new Counter('token_renewals');

export const options = {
  summaryTrendStats: ['avg', 'min', 'med', 'max', 'p(90)', 'p(95)', 'p(99)'],
  scenarios: { sustained: { executor: 'constant-vus', vus: VUS, duration: DURATION } },
  thresholds: {
    unhandled: ['rate<0.01'],
    'http_req_duration{endpoint:hazards}': ['p(95)<300'],
    'http_reqs{endpoint:hazards}': ['count>0'],
  },
};

// module scope is per VU in k6: each VU keeps its own token pair
let access = null;
let refresh = null;
let expiresAt = 0;

function store(res) {
  const b = res.json();
  access = b.access_token;
  refresh = b.refresh_token;
  expiresAt = Date.now() + b.expires_in * 1000;
}

function login() {
  const res = http.post(`${AUTH}/v1/token`, JSON.stringify({ client_id: CLIENT_ID, client_secret: CLIENT_SECRET }),
    { headers: { 'Content-Type': 'application/json' }, tags: { endpoint: 'auth' } });
  if (res.status !== 200) throw new Error(`login failed: ${res.status} ${res.body}`);
  store(res);
}

function ensureToken() {
  if (access === null) return login();
  if (Date.now() < expiresAt - RENEW_MARGIN_MS) return;
  const res = http.post(`${AUTH}/v1/refresh`, JSON.stringify({ refresh_token: refresh }),
    { headers: { 'Content-Type': 'application/json' }, tags: { endpoint: 'auth' } });
  if (res.status === 200) { store(res); renewals.add(1); } else login();
}

export default function () {
  if (!CLIENT_ID || !CLIENT_SECRET) throw new Error('K6_CLIENT_ID and K6_CLIENT_SECRET are required');
  ensureToken();
  const res = http.get(`${BASE}${PATH}`, {
    headers: { Authorization: `Bearer ${access}` }, timeout: '10s', tags: { endpoint: 'hazards' },
  });
  if (res.status === 200) ok200.add(1);
  else if (res.status === 429) shed429.add(1);
  else other.add(1);
  unhandled.add(res.status !== 200 && res.status !== 429);
  sleep(THINK_S);
}

export function handleSummary(data) {
  const m = data.metrics;
  const dur = m['http_req_duration{endpoint:hazards}'] && m['http_req_duration{endpoint:hazards}'].values;
  const reqs = m['http_reqs{endpoint:hazards}'] && m['http_reqs{endpoint:hazards}'].values;
  const cnt = (name) => (m[name] && m[name].values.count) || 0;
  const total = reqs ? reqs.count : 0;
  const out = {
    config: { vus: VUS, duration: DURATION, think_time_s: THINK_S, path: PATH, tool: 'k6' },
    requests: total,
    throughput_rps: reqs && reqs.rate,
    latency_ms: dur && { p50: dur.med, p95: dur['p(95)'], p99: dur['p(99)'], max: dur.max },
    status: { ok_200: cnt('resp_200'), rejected_429: cnt('resp_429'), other: cnt('resp_other') },
    rate_429: total ? cnt('resp_429') / total : 0,
    unhandled_error_rate: m.unhandled && m.unhandled.values.rate,
    token_renewals: cnt('token_renewals'),
  };
  const text = JSON.stringify(out, null, 2) + '\n';
  return { stdout: text, [SUMMARY]: text };
}
