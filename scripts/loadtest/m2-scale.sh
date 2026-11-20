#!/usr/bin/env bash
# M2 Problem 2 criterion 6: sustained load (>= 10 000 VU, >= 120 s) while the Aggregator is scaled 3 -> 5 -> 2;
# error rate outside controlled rejections < 1 %, and criterion 2 (one poller, no duplicates) still holds.
#
#   bash scripts/loadtest/m2-scale.sh            (transcript: docs/evidence/m2/p2-loadtest-scale.log)
#
# Runs k6 on THIS machine (k6 binary, or the grafana/k6 image) against the NodePorts of every node, so the load
# is spread over the nodes and never generated inside an application pod (Lampiran B). 10 000 VUs need a
# machine with ~2-3 GB free RAM and a high open-file limit (ulimit -n 65535); if the laptop that runs the VMs
# cannot, run this script from another machine on the same network with the cluster's kubeconfig.
#
# Test configuration (applied for the run, restored at the end):
#   - client-api: LOADTEST_CLIENT_API_REPLICAS pods (default 4) and a per-pod admission rate of LOADTEST_CLIENT_RPS
#     (default 2500) so that the Client-Facing API is not what is measured; Media is still shed first.
#   - traffic mix: K6_MEDIA_SHARE (default 0.9) of the VUs are Media, the rest Tim Lapangan. Beyond capacity the
#     admission control sheds Media with 429 (controlled rejection) and keeps serving Tim Lapangan (P5 criterion 5).
#
# Env: K6_VUS (10000) K6_RAMP (30s) K6_HOLD (150s) K6_THINK_S (1) SCALE_AT_5 (s after ramp, 30) SCALE_AT_2 (90)
#      CLIENT_API_URLS / AUTH_URL (default: NodePorts of every node)
. "$(dirname "${BASH_SOURCE[0]}")/../k8s/common.sh"
load_env
mkdir -p "$ROOT/docs/evidence/m2"
LOG="${LOG:-$ROOT/docs/evidence/m2/p2-loadtest-scale.log}"
exec > >(tee "$LOG") 2>&1

K6_VUS="${K6_VUS:-10000}" K6_RAMP="${K6_RAMP:-30s}" K6_HOLD="${K6_HOLD:-150s}" K6_THINK_S="${K6_THINK_S:-1}"
SCALE_AT_5="${SCALE_AT_5:-30}" SCALE_AT_2="${SCALE_AT_2:-90}"
REPLICAS_API="${LOADTEST_CLIENT_API_REPLICAS:-4}" RPS="${LOADTEST_CLIENT_RPS:-2500}"
secs() { local d=$1; case $d in *m) echo $((${d%m} * 60)) ;; *s) echo "${d%s}" ;; *) echo "$d" ;; esac; }
ramp_s=$(secs "$K6_RAMP") hold_s=$(secs "$K6_HOLD")
[ "$hold_s" -ge 120 ] || die "K6_HOLD must be >= 120s (criterion 6)"

nodes=$(kc get nodes -o jsonpath='{range .items[*]}{.status.addresses[?(@.type=="InternalIP")].address}{" "}{end}')
if [ -z "${CLIENT_API_URLS:-}" ]; then
  CLIENT_API_URLS=$(for ip in $nodes; do printf 'http://%s:30080,' "$ip"; done | sed 's/,$//')
fi
AUTH_URL="${AUTH_URL:-http://$(echo "$nodes" | awk '{print $1}'):30084}"
export CLIENT_API_URLS AUTH_URL K6_VUS K6_RAMP K6_HOLD K6_THINK_S K6_MODE=scale
export K6_CLIENT_ID="${K6_CLIENT_ID:-$AUTH_LAPANGAN_CLIENT_ID}" K6_CLIENT_SECRET="${K6_CLIENT_SECRET:-$AUTH_LAPANGAN_CLIENT_SECRET}"
export K6_MEDIA_ID="${K6_MEDIA_ID:-$AUTH_MEDIA_CLIENT_ID}" K6_MEDIA_SECRET="${K6_MEDIA_SECRET:-$AUTH_MEDIA_CLIENT_SECRET}"
export K6_MEDIA_SHARE="${K6_MEDIA_SHARE:-0.9}"
export K6_SUMMARY="docs/evidence/m2/k6-m2-scale-summary.json"

say "load test: $K6_VUS VUs, ramp $K6_RAMP, hold $K6_HOLD -> $CLIENT_API_URLS (k6 outside the cluster)"
kn get hpa aggregator >/dev/null 2>&1 && die "an HPA manages the Aggregator (bonus); switch it off first: bash scripts/k8s/hpa.sh off"
orig_api=$(kn get deploy client-api -o jsonpath='{.spec.replicas}')
restore() {
  say "restore: aggregator 3 replicas, client-api $orig_api replicas and its ConfigMap admission values"
  kn scale deploy/aggregator --replicas=3 >/dev/null || true
  kn set env deploy/client-api CLIENT_RATE_LIMIT_RPS- CLIENT_RATE_LIMIT_BURST- >/dev/null || true
  kn scale deploy/client-api --replicas="$orig_api" >/dev/null || true
}
trap restore EXIT

say "test configuration: client-api x$REPLICAS_API, $RPS req/s per pod; aggregator starts at 3"
kn set env deploy/client-api CLIENT_RATE_LIMIT_RPS="$RPS" CLIENT_RATE_LIMIT_BURST=$((RPS * 2)) >/dev/null
kn scale deploy/client-api --replicas="$REPLICAS_API" >/dev/null
kn scale deploy/aggregator --replicas=3 >/dev/null
kn rollout status deploy/client-api --timeout=180s
kn rollout status deploy/aggregator --timeout=180s
kn get pods -l 'app in (aggregator,client-api)' -o wide

run_k6() {
  if command -v k6 >/dev/null 2>&1; then
    (cd "$ROOT" && k6 run --quiet scripts/loadtest/m2.js)
  else
    docker run --rm -i --network host --ulimit nofile=65535:65535 \
      -e CLIENT_API_URLS -e AUTH_URL -e K6_CLIENT_ID -e K6_CLIENT_SECRET -e K6_VUS -e K6_RAMP -e K6_HOLD \
      -e K6_THINK_S -e K6_MODE -e K6_SUMMARY -e K6_MEDIA_ID -e K6_MEDIA_SECRET -e K6_MEDIA_SHARE -v "$ROOT:/work" -w /work grafana/k6:0.54.0 run --quiet scripts/loadtest/m2.js
  fi
}

started=$(date -u +%s)
start_iso=$(date -u +%FT%TZ)
run_k6 &
k6pid=$!
at() { # at <seconds since start> <command...>
  local t=$1; shift
  while [ $(($(date -u +%s) - started)) -lt "$t" ]; do kill -0 "$k6pid" 2>/dev/null || return 0; sleep 1; done
  printf '[t+%3ss %s] %s\n' "$(($(date -u +%s) - started))" "$(date -u +%T)" "$*"
  "$@"
}
watch_ready() {
  while kill -0 "$k6pid" 2>/dev/null; do
    printf '[t+%3ss] aggregator ready %s/%s\n' "$(($(date -u +%s) - started))" \
      "$(kn get deploy aggregator -o jsonpath='{.status.readyReplicas}')" "$(kn get deploy aggregator -o jsonpath='{.spec.replicas}')"
    sleep 10
  done
}
watch_ready &
at $((ramp_s + SCALE_AT_5)) kn scale deploy/aggregator --replicas=5
at $((ramp_s + SCALE_AT_2)) kn scale deploy/aggregator --replicas=2
wait "$k6pid"
k6rc=$?
ended=$(date -u +%s)

say "k6 summary ($K6_SUMMARY)"
cat "$ROOT/$K6_SUMMARY"
rate=$(sed -n 's/.*"error_rate": *\([0-9.e-]*\).*/\1/p' "$ROOT/$K6_SUMMARY")
awk -v r="$rate" 'BEGIN{exit !(r < 0.01)}' || die "error rate $rate >= 1 % (k6 exit $k6rc)"
printf 'PASS error rate %s < 1 %% outside controlled 429s, while scaling 3 -> 5 -> 2\n' "$rate"

say "criterion 2 during the test window ($start_iso .. $(date -u -d "@$ended" +%FT%TZ 2>/dev/null || date -u -r "$ended" +%FT%TZ))"
SINCE="$(((ended - started) / 60 + 2))m" bash "$ROOT/scripts/demo/m2-single-writer.sh"
