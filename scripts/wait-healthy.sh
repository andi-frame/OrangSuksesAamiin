#!/usr/bin/env bash
# Wait until every HTTP service of the stack answers /health (used by `make up` and CI).
set -uo pipefail
cd "$(dirname "$0")/.."
. scripts/winenv.sh
[ -f .env ] && { set -a; . ./.env; set +a; }

TIMEOUT="${1:-180}"
urls=(
  "http://localhost:${BMKG_PORT:-8081}/health"
  "http://localhost:${PVMBG_PORT:-8082}/health"
  "http://127.0.0.1:${AGGREGATOR_PORT:-8083}/health"
  "http://localhost:${AUTH_SERVICE_PORT:-8084}/health"
  "http://localhost:${CLIENT_API_PORT:-8080}/health"
  "http://localhost:${CONSUMER_DASHBOARD_PORT:-8091}/health"
  "http://localhost:${CONSUMER_NOTIFIER_PORT:-8092}/health"
)
deadline=$((SECONDS + TIMEOUT))
for u in "${urls[@]}"; do
  until [ "$(curl -s -o /dev/null -w '%{http_code}' --max-time 3 "$u")" = "200" ]; do
    if [ "$SECONDS" -ge "$deadline" ]; then
      echo "timeout waiting for $u" >&2
      docker compose ps >&2
      exit 1
    fi
    sleep 2
  done
  echo "ready: $u"
done
