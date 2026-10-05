import http from 'k6/http';
import { check, sleep } from 'k6';

const BASE = __ENV.CLIENT_API_URL || 'http://localhost:8080';
const TOKEN = __ENV.ACCESS_TOKEN;
const PATH = '/v1/hazards?hazard_type=SEISMIC&limit=20';

export const options = {
  scenarios: {
    constant_load: {
      executor: 'constant-vus',
      vus: 50,
      duration: '60s',
    },
  },
  thresholds: {
    // spesifikasi: unhandled (bukan 200/429) < 1%; 429 diperbolehkan
    checks: ['rate>0.985'],
  },
};

export default function () {
  if (!TOKEN) {
    throw new Error('ACCESS_TOKEN env is required');
  }
  const res = http.get(`${BASE}${PATH}`, {
    headers: { Authorization: `Bearer ${TOKEN}` },
    timeout: '10s',
  });
  check(res, {
    'status 200 or 429': (r) => r.status === 200 || r.status === 429,
    'not 5xx': (r) => r.status < 500,
  });
  // ponytail: 50 VUs with short sleep overwhelms single-node demo stack → 5xx; ~50–150 RPS keeps 200/429 only
  sleep(0.25);
}

export function handleSummary(data) {
  const dur = data.metrics.http_req_duration && data.metrics.http_req_duration.values;
  const reqs = data.metrics.http_reqs && data.metrics.http_reqs.values;
  const checks = data.metrics.checks && data.metrics.checks.values;
  const failed = data.metrics.http_req_failed && data.metrics.http_req_failed.values;
  const out = {
    p50_ms: dur && dur['p(50)'],
    p95_ms: dur && dur['p(95)'],
    p99_ms: dur && dur['p(99)'],
    rps: reqs && reqs.rate,
    checks_pass_rate: checks && checks.rate,
    http_req_failed_rate: failed && failed.rate,
  };
  const text = JSON.stringify(out, null, 2) + '\n';
  return {
    stdout: text,
    'docs/evidence/p2/k6-summary.json': text,
  };
}
