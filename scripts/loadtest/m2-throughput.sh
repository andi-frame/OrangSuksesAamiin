#!/usr/bin/env bash
# M2 Problem 2 criterion 7: throughput with 3 Aggregator instances >= 2.0 x throughput with 1 instance.
#
#   bash scripts/loadtest/m2-throughput.sh       (transcript: docs/evidence/m2/p2-loadtest-throughput.log)
#
# Same read path and k6 script as criterion 6 (scripts/loadtest/m2.js), in "max" mode: a fixed number of VUs
# without think time, so the achieved rate of 200 responses IS the capacity. Both runs use identical settings;
# only the number of Aggregator instances changes:
#   - every Aggregator pod gets the same CPU limit AGG_CPU_LIMIT (default 500m; Lampiran B leaves CPU free),
#     so "one instance" is one fixed unit of capacity and not "all cores of a node";
#   - the Client-Facing API runs LOADTEST_CLIENT_API_REPLICAS pods (default 4) with a high admission rate, so it is
#     not the bottleneck; the k6 VUs are Tim Lapangan (never shed).
# Everything is restored at the end.
#
# Env: MAX_VUS (400) RUN (60s) WARMUP_S (15) AGG_CPU_LIMIT (500m)
. "$(dirname "${BASH_SOURCE[0]}")/../k8s/common.sh"
load_env
mkdir -p "$ROOT/docs/evidence/m2"
LOG="${LOG:-$ROOT/docs/evidence/m2/p2-loadtest-throughput.log}"
exec > >(tee "$LOG") 2>&1

MAX_VUS="${MAX_VUS:-400}" RUN="${RUN:-60s}" WARMUP_S="${WARMUP_S:-15}" CPU="${AGG_CPU_LIMIT:-500m}"
REPLICAS_API="${LOADTEST_CLIENT_API_REPLICAS:-4}"
nodes=$(kc get nodes -o jsonpath='{range .items[*]}{.status.addresses[?(@.type=="InternalIP")].address}{" "}{end}')
CLIENT_API_URLS="${CLIENT_API_URLS:-$(for ip in $nodes; do printf 'http://%s:30080,' "$ip"; done | sed 's/,$//')}"
AUTH_URL="${AUTH_URL:-http://$(echo "$nodes" | awk '{print $1}'):30084}"
export CLIENT_API_URLS AUTH_URL K6_MODE=max K6_VUS="$MAX_VUS" K6_HOLD="$RUN" K6_THINK_S=0
export K6_CLIENT_ID="${K6_CLIENT_ID:-$AUTH_LAPANGAN_CLIENT_ID}" K6_CLIENT_SECRET="${K6_CLIENT_SECRET:-$AUTH_LAPANGAN_CLIENT_SECRET}"

kn get hpa aggregator >/dev/null 2>&1 && die "an HPA manages the Aggregator (bonus); switch it off first: bash scripts/k8s/hpa.sh off"
orig_api=$(kn get deploy client-api -o jsonpath='{.spec.replicas}')
restore() {
  say "restore: aggregator 3 replicas without the test CPU limit, client-api $orig_api replicas"
  kn patch deploy/aggregator --type=json \
    -p='[{"op":"remove","path":"/spec/template/spec/containers/0/resources/limits/cpu"}]' >/dev/null 2>&1 || true
  kn scale deploy/aggregator --replicas=3 >/dev/null || true
  kn set env deploy/client-api CLIENT_RATE_LIMIT_RPS- CLIENT_RATE_LIMIT_BURST- CLIENT_MAX_IN_FLIGHT- >/dev/null || true
  kn scale deploy/client-api --replicas="$orig_api" >/dev/null || true
}
trap restore EXIT

say "test configuration: aggregator cpu limit $CPU per pod, client-api x$REPLICAS_API (not the bottleneck), $MAX_VUS VUs x $RUN, no think time"
kn set resources deploy/aggregator --limits=cpu="$CPU" >/dev/null
kn set env deploy/client-api CLIENT_RATE_LIMIT_RPS=100000 CLIENT_RATE_LIMIT_BURST=200000 CLIENT_MAX_IN_FLIGHT=8192 >/dev/null
kn scale deploy/client-api --replicas="$REPLICAS_API" >/dev/null
kn rollout status deploy/client-api --timeout=180s

measure() { # measure <replicas>
  local n=$1
  say "aggregator x$n"
  kn scale deploy/aggregator --replicas="$n" >/dev/null
  kn rollout status deploy/aggregator --timeout=240s
  for _ in $(seq 1 60); do
    [ "$(kn get deploy aggregator -o jsonpath='{.status.readyReplicas}')" = "$n" ] &&
      [ "$(kn get pods -l app=aggregator --no-headers | wc -l | tr -d ' ')" = "$n" ] && break
    sleep 2
  done
  kn get pods -l app=aggregator -o wide
  sleep "$WARMUP_S"
  export K6_SUMMARY="docs/evidence/m2/k6-m2-throughput-$n.json"
  if command -v k6 >/dev/null 2>&1; then
    (cd "$ROOT" && k6 run --quiet scripts/loadtest/m2.js) || true
  else
    docker run --rm -i --network host --ulimit nofile=65535:65535 -e CLIENT_API_URLS -e AUTH_URL -e K6_CLIENT_ID \
      -e K6_CLIENT_SECRET -e K6_VUS -e K6_HOLD -e K6_THINK_S -e K6_MODE -e K6_SUMMARY -v "$ROOT:/work" -w /work \
      grafana/k6:0.54.0 run --quiet scripts/loadtest/m2.js || true
  fi
  sed -n 's/.*"throughput_ok_rps": *\([0-9.e+-]*\).*/\1/p' "$ROOT/$K6_SUMMARY"
}

t1=$(measure 1 | tail -1)
t3=$(measure 3 | tail -1)
say "result"
awk -v a="$t1" -v b="$t3" 'BEGIN{
  r = (a > 0) ? b / a : 0
  printf "   1 instance : %10.1f ok req/s\n   3 instances: %10.1f ok req/s\n   ratio      : %10.2f (criterion: >= 2.0)\n", a, b, r
  if (r >= 2.0) { print "PASS"; exit 0 } else { print "FAIL"; exit 1 } }'
