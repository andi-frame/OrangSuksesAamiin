#!/usr/bin/env bash
# Run the P2 k6 load test (50 VUs x 60 s by default) against the Client-Facing API.
# Overrides: K6_VUS, K6_DURATION, K6_THINK_S, K6_SUMMARY (output json), K6_LOG.
# Credentials are read from the environment (K6_CLIENT_ID / K6_CLIENT_SECRET, default: the Lapangan client of .env);
# each VU logs in and renews its own token, so no token is passed in.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT"
mkdir -p docs/evidence/p2
[ -f .env ] && { set -a; . ./.env; set +a; }

export K6_CLIENT_ID="${K6_CLIENT_ID:-${AUTH_LAPANGAN_CLIENT_ID:-}}"
export K6_CLIENT_SECRET="${K6_CLIENT_SECRET:-${AUTH_LAPANGAN_CLIENT_SECRET:-}}"
if [ -z "$K6_CLIENT_ID" ] || [ -z "$K6_CLIENT_SECRET" ]; then
  echo "set K6_CLIENT_ID / K6_CLIENT_SECRET or AUTH_LAPANGAN_CLIENT_ID / _SECRET (see .env.example)" >&2
  exit 1
fi
export CLIENT_API_URL="${CLIENT_API_URL:-http://localhost:${CLIENT_API_PORT:-8080}}"
export AUTH_URL="${AUTH_URL:-http://localhost:${AUTH_SERVICE_PORT:-8084}}"

if command -v k6 >/dev/null 2>&1; then
  k6 run --quiet scripts/loadtest/p2.js 2>&1 | tee "${K6_LOG:-docs/evidence/p2/k6.log}"
  exit "${PIPESTATUS[0]}"
fi

if command -v docker >/dev/null 2>&1; then
  EXTRA=()
  if [ "$(uname -s)" = Linux ]; then
    EXTRA=(--network host)
  else
    CLIENT_API_URL="${CLIENT_API_URL/localhost/host.docker.internal}"
    AUTH_URL="${AUTH_URL/localhost/host.docker.internal}"
    EXTRA=(--add-host=host.docker.internal:host-gateway)
  fi
  MSYS_NO_PATHCONV=1 docker run --rm -i "${EXTRA[@]}" \
    -e K6_CLIENT_ID -e K6_CLIENT_SECRET -e CLIENT_API_URL -e AUTH_URL -e K6_VUS -e K6_DURATION -e K6_THINK_S -e K6_SUMMARY \
    -v "$ROOT:/work" -w /work \
    grafana/k6:0.54.0 run --quiet scripts/loadtest/p2.js 2>&1 | tee "${K6_LOG:-docs/evidence/p2/k6.log}"
  exit "${PIPESTATUS[0]}"
fi

echo "k6 not found and docker unavailable" >&2
exit 1
