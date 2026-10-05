#!/usr/bin/env bash
# Run P2 k6 load test. Usage: run-p2.sh [ACCESS_TOKEN]
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT"
mkdir -p docs/evidence/p2

TOKEN="${1:-${ACCESS_TOKEN:-}}"
if [ -z "$TOKEN" ]; then
  echo "usage: ACCESS_TOKEN=... $0   OR   $0 <token>" >&2
  exit 1
fi

export ACCESS_TOKEN="$TOKEN"
export CLIENT_API_URL="${CLIENT_API_URL:-http://localhost:8080}"

run_k6() {
  k6 run scripts/loadtest/p2.js
}

if command -v k6 >/dev/null 2>&1; then
  run_k6 2>&1 | tee docs/evidence/p2/k6.log
  exit "${PIPESTATUS[0]}"
fi

if command -v docker >/dev/null 2>&1; then
  EXTRA=()
  API_URL="$CLIENT_API_URL"
  if [[ "$API_URL" == *localhost* ]] && [[ "$(uname -s)" == Darwin ]]; then
    API_URL="${API_URL/localhost/host.docker.internal}"
    EXTRA=(--add-host=host.docker.internal:host-gateway)
  elif [[ "$API_URL" == *localhost* ]] && [[ "$(uname -s)" == Linux ]]; then
    EXTRA=(--network host)
    API_URL="${CLIENT_API_URL}"
  fi
  export CLIENT_API_URL="$API_URL"
  docker run --rm -i "${EXTRA[@]}" \
    -e ACCESS_TOKEN -e CLIENT_API_URL \
    -v "$ROOT:/work" -w /work \
    grafana/k6:0.54.0 run scripts/loadtest/p2.js 2>&1 | tee docs/evidence/p2/k6.log
  exit "${PIPESTATUS[0]}"
fi

echo "k6 not found and docker unavailable" >&2
exit 1
