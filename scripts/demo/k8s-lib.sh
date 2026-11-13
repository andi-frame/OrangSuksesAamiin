#!/usr/bin/env bash
# Helpers of the M2 (Kubernetes) demos. Sourced, not run.
# Reaches the cluster with kubectl only (port-forward / exec), so it works the same on k3s VMs and kind.
. "$(dirname "${BASH_SOURCE[0]}")/../k8s/common.sh"
load_env

C_B=$'\033[1m' C_C=$'\033[36m' C_G=$'\033[32m' C_Y=$'\033[33m' C_R=$'\033[31m' C_0=$'\033[0m'
STEP=0
step() { STEP=$((STEP + 1)); printf '\n%s== [%s] %s ==%s\n' "$C_B$C_C" "$STEP" "$*" "$C_0"; }
info() { printf '   %s\n' "$*"; }
ok()   { printf '   %sPASS%s %s\n' "$C_G" "$C_0" "$*"; }
warn() { printf '   %sWARN%s %s\n' "$C_Y" "$C_0" "$*"; }
fail() { printf '   %sFAIL%s %s\n' "$C_R" "$C_0" "$*"; exit 1; }
show() { printf '%s$ %s%s\n' "$C_Y" "$*" "$C_0"; }

# jq.exe on Windows ends lines with CRLF: strip the CR so ids compare equal
if command -v jq >/dev/null 2>&1; then jqx() { jq "$@" | tr -d '\r'; }; else jqx() { docker run --rm -i ghcr.io/jqlang/jq:1.7.1 "$@"; }; fi

PF_PIDS=()
cleanup_pf() { for p in "${PF_PIDS[@]:-}"; do [ -n "$p" ] && kill "$p" 2>/dev/null || true; done; }
trap cleanup_pf EXIT

# forward <service> <local-port> <remote-port>: port-forward in the background until the demo ends.
# A forward is bound to one pod; when that pod goes away (rollout, kill) ensure_forwards restarts it.
declare -A PF_SPEC=()
declare -A PF_PID=()
forward() {
  PF_SPEC[$2]="$1 $3"
  kn port-forward "svc/$1" "$2:$3" >/dev/null 2>&1 &
  PF_PID[$2]=$!
  PF_PIDS+=("$!")
  for _ in $(seq 1 50); do
    curl -s -o /dev/null --max-time 1 "http://127.0.0.1:$2/health" && return 0
    sleep 0.2
  done
  fail "port-forward to $1 did not come up"
}
ensure_forwards() {
  local port svc rport
  for port in "${!PF_SPEC[@]}"; do
    curl -s -o /dev/null --max-time 2 "http://127.0.0.1:$port/health" && continue
    kill "${PF_PID[$port]}" 2>/dev/null || true
    read -r svc rport <<<"${PF_SPEC[$port]}"
    forward "$svc" "$port" "$rport"
  done
}

AGG=http://127.0.0.1:18083
PVMBG=http://127.0.0.1:18082
BMKG=http://127.0.0.1:18081

agg()   { ensure_forwards; curl -sS --max-time 15 -H "X-Service-Key: $AGGREGATOR_SERVICE_KEY" "$AGG$1"; }
ops()   { ensure_forwards; curl -sS --max-time 15 -H "Authorization: Bearer $AGGREGATOR_OPS_TOKEN" "$AGG$1"; }
ops_post() { ensure_forwards; curl -sS --max-time 15 -X POST -H "Authorization: Bearer $AGGREGATOR_OPS_TOKEN" -H 'Content-Type: application/json' -d "${2:-{\}}" "$AGG$1"; }
audit() { ensure_forwards; curl -sS --max-time 15 -H "Authorization: Bearer $AUDIT_TOKEN" "$AGG$1"; }
code()  { ensure_forwards; curl -s -o /dev/null -w '%{http_code}' --max-time 15 "$@"; }
# agg_has <path>: the Aggregator answers 200 on path (usable in wait_until)
agg_has() { [ "$(code -H "X-Service-Key: $AGGREGATOR_SERVICE_KEY" "$AGG$1")" = 200 ]; }
pvmbg_admin() { ensure_forwards; curl -sS --max-time 15 -X POST -H "X-Admin-Token: $PVMBG_ADMIN_TOKEN" -H 'Content-Type: application/json' -d "${2:-{\}}" "$PVMBG$1"; }
bmkg_admin()  { ensure_forwards; curl -sS --max-time 15 -X POST -H "X-Admin-Token: $BMKG_ADMIN_TOKEN" -H 'Content-Type: application/json' -d "${2:-{\}}" "$BMKG$1"; }

# psql runs SQL in the Canonical Store pod (the only way in from outside: kubectl exec)
psql_q() {
  kn exec postgres-canonical-0 -c postgres -- sh -c 'psql -X -q -A -t -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "$1"' _ "$1" | tr -d '\r'
}
psql_table() {
  kn exec postgres-canonical-0 -c postgres -- sh -c 'psql -X -q -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "$1"' _ "$1" | tr -d '\r'
}

# retried: a node freezing or a partition can make single API calls fail, and under set -e a failed
# $(leader) would end the demo silently
lease_field() { local i; for i in 1 2 3 4 5; do kn get lease aggregator-ingest -o jsonpath="{.spec.$1}" 2>/dev/null && return 0; sleep 2; done; return 1; }
leader() { lease_field holderIdentity; }
lease_term() { lease_field leaseTransitions; }

# wait_until <seconds> <description> <command...>
wait_until() {
  local deadline=$((SECONDS + $1)) what=$2
  shift 2
  until "$@" >/dev/null 2>&1; do
    [ $SECONDS -ge $deadline ] && fail "timed out waiting for: $what"
    sleep 1
  done
}

# restarts <app>: total container restarts of the app's pods
restarts() { kn get pods -l "app=$1" -o jsonpath='{range .items[*]}{.status.containerStatuses[0].restartCount}{"\n"}{end}' | awk '{s+=$1} END {print s+0}'; }

# upstream_callers <mock> <since>: the upstream's request log (one clock) cut into runs of consecutive requests
# from the same caller, oldest first: one run per leadership period (an instance that leads twice has two runs).
# Output: first_epoch last_epoch n caller first last
upstream_callers() {
  kn logs "deploy/$1" --since="$2" | grep '"msg":"request"' | grep -v '"/health"' | grep -v '/admin/' |
    jqx -rs 'def epoch: capture("^(?<s>[^.Z]+)(\\.(?<f>[0-9]+))?Z$") as $c
                        | ($c.s + "Z" | fromdateiso8601) + ("0." + ($c.f // "0") | tonumber);
             map(select((.caller // "") != "") | {caller, ts, e: (.ts | epoch)}) | sort_by(.e)
             | reduce .[] as $r ([];
                 if length > 0 and .[-1].caller == $r.caller
                 then .[-1].n += 1 | .[-1].hi = $r.e | .[-1].last = $r.ts
                 else . + [{caller: $r.caller, n: 1, lo: $r.e, hi: $r.e, first: $r.ts, last: $r.ts}] end)
             | .[] | "\(.lo) \(.hi) \(.n) \(.caller) \(.first) \(.last)"'
}
# overlapping: callers that interleave - a caller whose run resumes less than one lease duration (15 s) after
# its previous run ended, with another caller in between. Two instances polling at the same time show up as
# A,B,A,B runs a poll interval apart; a real handover leaves the old leader silent for at least a lease.
overlapping() { awk -v lease="${LEASE_SEC:-15}" '($4 in end) && $1 - end[$4] < lease {bad++} {end[$4] = $2} END {print bad + 0}'; }

# term_periods: per leadership term, writes and first/last write on the STORE's clock
term_periods() {
  psql_table "SELECT writer_term AS term, writer, count(*) AS writes, min(stored_at) AS first_write, max(stored_at) AS last_write
    FROM outbox WHERE writer_term IS NOT NULL GROUP BY 1,2 ORDER BY 1"
}
# term_overlaps: pairs of terms whose write periods overlap (must be 0)
term_overlaps() {
  psql_q "WITH t AS (SELECT writer_term, min(stored_at) lo, max(stored_at) hi FROM outbox WHERE writer_term IS NOT NULL GROUP BY 1)
    SELECT count(*) FROM t a JOIN t b ON a.writer_term < b.writer_term AND a.hi > b.lo"
}

# settled: the rollout is over - only current pods exist (none terminating) and one of them holds the lease.
# Requeue dead letters only then: an old pod can lead until its preStop ends, and would retry with the old rules.
settled() {
  local want terminating l
  want=$(kn get deploy aggregator -o jsonpath='{.spec.replicas}')
  terminating=$(kn get pods -l app=aggregator -o jsonpath='{range .items[?(@.metadata.deletionTimestamp)]}x{end}')
  [ -z "$terminating" ] || return 1
  [ "$(kn get pods -l app=aggregator --field-selector=status.phase=Running -o name | wc -l | tr -d ' ')" = "$want" ] || return 1
  l=$(leader)
  [ -n "$l" ] && kn get pod "$l" >/dev/null 2>&1
}
