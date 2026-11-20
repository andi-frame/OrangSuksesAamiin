// M2 Problem 2 load test (criteria 6 and 7), run by scripts/loadtest/m2-scale.sh and m2-throughput.sh.
//
// Target: the scaled read path  client (k6) -> Client-Facing API (NodePort, every node) -> Aggregator
// Service -> /internal/hazards. Ingest stays on the single leader; what is scaled is the number of
// Aggregator instances that serve reads.
//
// Every VU is a Tim Lapangan client: it logs in once and renews its access token before it expires
// (POST /v1/refresh), like the M1 test. The login burst is spread over the ramp-up.
//
// Classification (spec "error rate di luar penolakan terkontrol"):
//   200        ok
//   429        controlled rejection (shed by admission control, with Retry-After) - reported, not an error
//   other      error: 5xx, 401/403, timeouts, connection errors - must stay below 1 %
//
// Env:
//   CLIENT_API_URLS   comma list, e.g. http://192.168.56.10:30080,http://192.168.56.11:30080 (VUs spread over them)
//   AUTH_URL          http://<node>:30084
//   K6_CLIENT_ID / K6_CLIENT_SECRET   a Tim Lapangan client (AUTH_LAPANGAN_CLIENT_* of .env)
//   K6_MEDIA_ID / K6_MEDIA_SECRET + K6_MEDIA_SHARE (0..1, default 0): that share of the VUs are Media clients
//     (shed first under overload, P5 criterion 5); per-role counts are in the summary
//   K6_VUS (10000)  K6_RAMP (30s)  K6_HOLD (150s)  K6_THINK_S (1)  K6_SUMMARY (json path)  K6_MODE (scale|max)
//   K6_MODE=max: no think time and a constant VU count (K6_VUS, K6_HOLD) to measure maximum throughput.
import http from 'k6/http';
import { sleep } from 'k6';
import { Counter, Rate, Trend } from 'k6/metrics';
import exec from 'k6/execution';

const URLS = (__ENV.CLIENT_API_URLS || 'http://localhost:8080').split(',').map((s) => s.trim()).filter(Boolean);
const AUTH = __ENV.AUTH_URL || 'http://localhost:8084';
const CLIENT_ID = __ENV.K6_CLIENT_ID;
const CLIENT_SECRET = __ENV.K6_CLIENT_SECRET;
const MEDIA_ID = __ENV.K6_MEDIA_ID;
const MEDIA_SECRET = __ENV.K6_MEDIA_SECRET;
const MEDIA_SHARE = MEDIA_ID ? parseFloat(__ENV.K6_MEDIA_SHARE || '0') : 0;
const VUS = parseInt(__ENV.K6_VUS || '10000');
const RAMP = __ENV.K6_RAMP || '30s';
const HOLD = __ENV.K6_HOLD || '150s';
const THINK_S = parseFloat(__ENV.K6_THINK_S || '1');
const MODE = __ENV.K6_MODE || 'scale';
const SUMMARY = __ENV.K6_SUMMARY || 'docs/evidence/m2/k6-m2-summary.json';
const RENEW_MARGIN_MS = 15000;
const QUERIES = ['/v1/hazards?limit=20', '/v1/hazards?hazard_type=SEISMIC&limit=20',
  '/v1/hazards?hazard_type=VOLCANIC&limit=20', '/v1/hazards?severity=SIAGA&limit=20'];

const ok200 = new Counter('resp_200');
const shed429 = new Counter('resp_429');
const errors = new Counter('resp_error');
const unhandled = new Rate('unhandled');
const authFail = new Counter('auth_failures');
const latency = new Trend('hazards_latency', true);
const byRole = { media: { ok: new Counter('media_200'), shed: new Counter('media_429'), err: new Counter('media_error') },
  lapangan: { ok: new Counter('lapangan_200'), shed: new Counter('lapangan_429'), err: new Counter('lapangan_error') } };

export const options = {
  discardResponseBodies: true,
  summaryTrendStats: ['avg', 'min', 'med', 'max', 'p(90)', 'p(95)', 'p(99)'],
  scenarios: MODE === 'max'
    ? { max: { executor: 'constant-vus', vus: VUS, duration: HOLD } }
    : {
      sustained: {
        executor: 'ramping-vus', startVUs: 0, gracefulRampDown: '15s',
        stages: [{ duration: RAMP, target: VUS }, { duration: HOLD, target: VUS }, { duration: '10s', target: 0 }],
      },
    },
  thresholds: {
    unhandled: ['rate<0.01'],
    'http_reqs{endpoint:hazards}': ['count>0'],
  },
  // 10 000 VUs: do not keep idle sockets per VU forever
  noVUConnectionReuse: false,
  batchPerHost: 0,
};

let access = null;
let refresh = null;
let expiresAt = 0;

function store(res) {
  const b = JSON.parse(res.body);
  access = b.access_token;
  refresh = b.refresh_token;
  expiresAt = Date.now() + b.expires_in * 1000;
}

function post(path, body) {
  return http.post(`${AUTH}${path}`, JSON.stringify(body), {
    headers: { 'Content-Type': 'application/json' }, tags: { endpoint: 'auth' },
    responseType: 'text', timeout: '20s',
  });
}

// the role of a VU is fixed by its id, so the mix is exact and stable over the run
function isMedia() { return (exec.vu.idInTest % 100) < MEDIA_SHARE * 100; }

function ensureToken() {
  if (access !== null && Date.now() < expiresAt - RENEW_MARGIN_MS) return true;
  let res = null;
  if (refresh !== null) res = post('/v1/refresh', { refresh_token: refresh });
  const media = isMedia();
  if (res === null || res.status !== 200) {
    res = post('/v1/token', media ? { client_id: MEDIA_ID, client_secret: MEDIA_SECRET } : { client_id: CLIENT_ID, client_secret: CLIENT_SECRET });
  }
  if (res.status !== 200) {
    authFail.add(1);
    sleep(1 + Math.random());
    return false;
  }
  store(res);
  return true;
}

export default function () {
  if (!ensureToken()) return;
  const base = URLS[exec.vu.idInTest % URLS.length];
  const q = QUERIES[exec.vu.iterationInScenario % QUERIES.length];
  const res = http.get(`${base}${q}`, {
    headers: { Authorization: `Bearer ${access}` }, tags: { endpoint: 'hazards' }, timeout: '10s',
  });
  latency.add(res.timings.duration);
  const role = byRole[isMedia() ? 'media' : 'lapangan'];
  if (res.status === 200) {
    ok200.add(1);
    role.ok.add(1);
    unhandled.add(false);
  } else if (res.status === 429) {
    shed429.add(1);
    role.shed.add(1);
    unhandled.add(false);
  } else {
    role.err.add(1);
    if (res.status === 401) access = null; // expired between check and use: renew, but it still counts
    errors.add(1, { status: String(res.status) });
    unhandled.add(true);
  }
  if (THINK_S > 0 && MODE !== 'max') sleep(THINK_S * (0.5 + Math.random()));
}

export function handleSummary(data) {
  const m = data.metrics;
  const v = (k, f) => (m[k] && m[k].values ? m[k].values[f] : 0);
  const secs = data.state.testRunDurationMs / 1000;
  const out = {
    mode: MODE, vus: VUS, ramp: RAMP, hold: HOLD, think_s: THINK_S, targets: URLS,
    duration_s: secs,
    requests: v('http_reqs{endpoint:hazards}', 'count') || (v('resp_200', 'count') + v('resp_429', 'count') + v('resp_error', 'count')),
    ok_200: v('resp_200', 'count'), shed_429: v('resp_429', 'count'), errors: v('resp_error', 'count'),
    error_rate: v('unhandled', 'rate'),
    throughput_ok_rps: v('resp_200', 'rate'),
    throughput_all_rps: v('http_reqs', 'rate'),
    latency_ms: { p50: v('hazards_latency', 'med'), p95: v('hazards_latency', 'p(95)'), p99: v('hazards_latency', 'p(99)'), max: v('hazards_latency', 'max') },
    auth_failures: v('auth_failures', 'count'),
    media_share: MEDIA_SHARE,
    by_role: {
      media: { ok_200: v('media_200', 'count'), shed_429: v('media_429', 'count'), errors: v('media_error', 'count') },
      lapangan: { ok_200: v('lapangan_200', 'count'), shed_429: v('lapangan_429', 'count'), errors: v('lapangan_error', 'count') },
    },
    vus_max: v('vus_max', 'max'),
  };
  const line = `mode=${out.mode} vus=${out.vus_max} duration=${secs.toFixed(0)}s ok=${out.ok_200} shed429=${out.shed_429} errors=${out.errors} ` +
    `error_rate=${(out.error_rate * 100).toFixed(3)}% ok_rps=${out.throughput_ok_rps.toFixed(1)} p95=${out.latency_ms.p95.toFixed(1)}ms\n`;
  return { stdout: line, [SUMMARY]: JSON.stringify(out, null, 2) };
}
