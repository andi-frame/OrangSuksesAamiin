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
export BMKG_URL PVMBG_URL AGG_URL

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

require_stack() {
  local u
  for u in "${BMKG_URL}/health" "${PVMBG_URL}/health" "${AGG_URL}/health"; do
    [ "$(code "$u")" = "200" ] || fail "$u is not answering - start the system first: make up"
  done
  command -v curl >/dev/null || fail "curl is required"
}

# started_at SERVICE -> container start time (proof that nothing was restarted)
started_at() {
  local id
  id="$(docker compose ps -q "$1")"
  [ -n "$id" ] || { echo "not-running"; return; }
  docker inspect -f '{{.State.StartedAt}}' "$id"
}
