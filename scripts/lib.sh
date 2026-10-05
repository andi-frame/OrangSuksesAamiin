#!/usr/bin/env bash
# Shared helpers for the demo scripts. Source it:  . "$(dirname "$0")/../lib.sh"
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck disable=SC1091
. "$ROOT/scripts/winenv.sh"
cd "$ROOT"

if [ -f "$ROOT/.env" ]; then
  set -a
  # shellcheck disable=SC1091
  . "$ROOT/.env"
  set +a
else
  echo "no .env found - run: cp .env.example .env" >&2
  exit 2
fi

BMKG_URL="http://localhost:${BMKG_PORT:-8081}"
PVMBG_URL="http://localhost:${PVMBG_PORT:-8082}"
AGG_URL="http://127.0.0.1:${AGGREGATOR_PORT:-8083}"
CAPI_URL="http://localhost:${CLIENT_API_PORT:-8080}"
AUTH_URL="http://localhost:${AUTH_SERVICE_PORT:-8084}"
export BMKG_URL PVMBG_URL AGG_URL CAPI_URL AUTH_URL

# ---- output ---------------------------------------------------------------
if [ -t 1 ]; then
  C_B=$'\033[1m'; C_G=$'\033[32m'; C_R=$'\033[31m'; C_Y=$'\033[33m'; C_C=$'\033[36m'; C_0=$'\033[0m'
else
  C_B=""; C_G=""; C_R=""; C_Y=""; C_C=""; C_0=""
fi
STEP=0
step()  { STEP=$((STEP + 1)); printf '\n%s== [%s] %s ==%s\n' "$C_B$C_C" "$STEP" "$*" "$C_0"; }
info()  { printf '   %s\n' "$*"; }
ok()    { printf '   %sPASS%s %s\n' "$C_G" "$C_0" "$*"; }
warn()  { printf '   %sWARN%s %s\n' "$C_Y" "$C_0" "$*"; }
fail()  { printf '   %sFAIL%s %s\n' "$C_R" "$C_0" "$*"; exit 1; }
show()  { printf '%s$ %s%s\n' "$C_Y" "$*" "$C_0"; }

# ---- jq (falls back to a container when jq is not installed) ------------------
jqx() {
  if command -v jq >/dev/null 2>&1; then
    jq "$@"
  else
    docker run --rm -i ghcr.io/jqlang/jq:1.7.1 "$@"
  fi
}

# ---- HTTP -----------------------------------------------------------------
# agg PATH            -> GET on the Aggregator internal API (with the service key)
agg() { curl -sS --max-time 15 -H "X-Service-Key: ${AGGREGATOR_SERVICE_KEY}" "${AGG_URL}$1"; }
# capi TOKEN PATH     -> GET on the Client-Facing API
capi() { curl -sS --max-time 15 -H "Authorization: Bearer ${1:?token}" "${CAPI_URL}$2"; }
# auth_token CLIENT_ID CLIENT_SECRET -> POST /v1/token (JSON access_token on stdout via jq in demos)
auth_token() {
  curl -sS --max-time 15 -X POST -H 'Content-Type: application/json' \
    -d "{\"client_id\":\"$1\",\"client_secret\":\"$2\"}" "${AUTH_URL}/v1/token"
}
# capi_code TOKEN PATH -> HTTP status from Client-Facing API
capi_code() {
  curl -s -o /dev/null -w '%{http_code}' --max-time 15 \
    -H "Authorization: Bearer ${1:?token}" "${CAPI_URL}$2"
}
# capi_p95_ms N TOKEN PATH -> prints "P95_MS HARD_ERROR_COUNT" (200 and 429 are OK)
capi_p95_ms() {
  local n="$1" tok="$2" path="$3" i
  for ((i = 0; i < n; i++)); do
    curl -s -o /dev/null -w '%{http_code} %{time_total}\n' --max-time 10 \
      -H "Authorization: Bearer ${tok}" "${CAPI_URL}${path}"
  done | awk '
    $1 != 200 && $1 != 429 { bad++ }
    { cnt++; t[cnt] = $2 * 1000 }
    END {
      for (i = 1; i <= cnt; i++)
        for (j = i + 1; j <= cnt; j++)
          if (t[i] > t[j]) { tmp = t[i]; t[i] = t[j]; t[j] = tmp }
      i = int(cnt * 0.95 + 0.999)
      if (i < 1) i = 1
      if (cnt < 1) { printf "0 0\n"; exit }
      printf "%.1f %d\n", t[i], bad + 0
    }'
}
# auth_refresh REFRESH_TOKEN -> POST /v1/refresh JSON body
auth_refresh() {
  curl -sS --max-time 15 -X POST -H 'Content-Type: application/json' \
    -d "{\"refresh_token\":\"$1\"}" "${AUTH_URL}/v1/refresh"
}
# auth_code METHOD PATH [JSON body] -> HTTP status on auth-service
auth_code() {
  local method="$1" path="$2" body="${3:-}"
  if [ -n "$body" ]; then
    curl -s -o /dev/null -w '%{http_code}' --max-time 15 -X "$method" \
      -H 'Content-Type: application/json' -d "$body" "${AUTH_URL}${path}"
  else
    curl -s -o /dev/null -w '%{http_code}' --max-time 15 -X "$method" "${AUTH_URL}${path}"
  fi
}
# bmkg PATH           -> GET on the BMKG mock with its own credential
bmkg() { curl -sS --max-time 15 -H "X-BMKG-Key: ${BMKG_API_KEY}" "${BMKG_URL}$1"; }
# pvmbg PATH          -> GET on the PVMBG mock with its own credential
pvmbg() { curl -sS --max-time 15 -H "Authorization: Bearer ${PVMBG_GATEWAY_TOKEN}" "${PVMBG_URL}$1"; }
# pvmbg_admin PATH JSON / bmkg_admin PATH JSON
pvmbg_admin() { curl -sS --max-time 15 -X POST -H "X-Admin-Token: ${PVMBG_ADMIN_TOKEN}" -H 'Content-Type: application/json' -d "${2:-{\}}" "${PVMBG_URL}$1"; }
bmkg_admin()  { curl -sS --max-time 15 -X POST -H "X-Admin-Token: ${BMKG_ADMIN_TOKEN}" -H 'Content-Type: application/json' -d "${2:-{\}}" "${BMKG_URL}$1"; }
# code URL [curl args] -> just the HTTP status code
code() { curl -s -o /dev/null -w '%{http_code}' --max-time 15 "$@"; }

# wait_until "description" timeout_seconds command...   (command succeeds => done)
wait_until() {
  local desc="$1" timeout="$2"; shift 2
  local deadline=$((SECONDS + timeout))
  until "$@" >/dev/null 2>&1; do
    if [ "$SECONDS" -ge "$deadline" ]; then fail "timed out after ${timeout}s waiting for: ${desc}"; fi
    sleep 1
  done
}

# retry ATTEMPTS DELAY command...   (for checks that race with ingestion)
retry() {
  local n="$1" d="$2"; shift 2
  local i
  for ((i = 1; i <= n; i++)); do
    if "$@"; then return 0; fi
    [ "$i" -lt "$n" ] && sleep "$d"
  done
  return 1
}

# Orang 2 demos (P2/P3): Client API + Auth + mocks + Aggregator — no Kafka consumers required.
require_client_stack() {
  local u
  for u in "${BMKG_URL}/health" "${PVMBG_URL}/health" "${AGG_URL}/health" "${AUTH_URL}/health" "${CAPI_URL}/health"; do
    [ "$(code "$u")" = "200" ] || fail "$u is not answering - start the system first: make up"
  done
  command -v curl >/dev/null || fail "curl is required"
}

require_stack() {
  require_client_stack
  local u
  for u in "http://localhost:${CONSUMER_DASHBOARD_PORT:-8091}/health" \
    "http://localhost:${CONSUMER_NOTIFIER_PORT:-8092}/health"; do
    [ "$(code "$u")" = "200" ] || fail "$u is not answering - start the system first: make up"
  done
}

# started_at SERVICE -> container start time (proof that nothing was restarted)
started_at() {
  local id
  id="$(docker compose ps -q "$1")"
  [ -n "$id" ] || { echo "not-running"; return; }
  docker inspect -f '{{.State.StartedAt}}' "$id"
}
