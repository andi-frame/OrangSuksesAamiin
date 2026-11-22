#!/usr/bin/env bash
# M2 Problem 2 criterion 6 with the load split over two generators, so that no generator shares its CPU with the
# cluster: half of the VUs from this machine (host-only NIC to the NodePorts), half from a remote machine (REMOTE,
# reached with ssh; it gets the NodePorts through an ssh reverse tunnel). Same k6 script and classification as
# m2-scale.sh; the Aggregator is scaled 3 -> 5 -> 2 during the hold phase, then criterion 2 is checked.
#
#   REMOTE=byhan bash scripts/loadtest/m2-scale-distributed.sh   (transcript: docs/evidence/m2/p2-loadtest-scale-distributed.log)
#
# Env: K6_VUS (total, 10000) K6_RAMP (60s) K6_HOLD (150s) K6_THINK_S (15) SCALE_AT_5 (30) SCALE_AT_2 (90)
#      LOCAL_URLS (http://192.168.56.11:30080,http://192.168.56.12:30080) LOCAL_AUTH (http://192.168.56.11:30084)
#      TUNNEL_TARGET (192.168.56.11): where the reverse tunnel ends on this side
. "$(dirname "${BASH_SOURCE[0]}")/../k8s/common.sh"
load_env
REMOTE="${REMOTE:-byhan}"
LOG="${LOG:-$ROOT/docs/evidence/m2/p2-loadtest-scale-distributed.log}"
exec > >(tee "$LOG") 2>&1
K6_VUS="${K6_VUS:-10000}" K6_RAMP="${K6_RAMP:-60s}" K6_HOLD="${K6_HOLD:-150s}" K6_THINK_S="${K6_THINK_S:-15}"
SCALE_AT_5="${SCALE_AT_5:-30}" SCALE_AT_2="${SCALE_AT_2:-90}"
LOCAL_URLS="${LOCAL_URLS:-http://192.168.56.11:30080,http://192.168.56.12:30080}"
LOCAL_AUTH="${LOCAL_AUTH:-http://192.168.56.11:30084}" TUNNEL_TARGET="${TUNNEL_TARGET:-192.168.56.11}"
half=$((K6_VUS / 2))
secs() { local d=$1; case $d in *m) echo $((${d%m} * 60)) ;; *s) echo "${d%s}" ;; *) echo "$d" ;; esac; }
ramp_s=$(secs "$K6_RAMP") hold_s=$(secs "$K6_HOLD")
[ "$hold_s" -ge 120 ] || die "K6_HOLD must be >= 120s (criterion 6)"
work=$(mktemp -d)

say "distributed load test: $K6_VUS VUs ($half here, $half on $REMOTE), ramp $K6_RAMP, hold $K6_HOLD, think ${K6_THINK_S}s"
kn get hpa aggregator >/dev/null 2>&1 && die "an HPA manages the Aggregator; switch it off first: bash scripts/k8s/hpa.sh off"
orig_api=$(kn get deploy client-api -o jsonpath='{.spec.replicas}')
cleanup() {
  [ -n "${tunnel:-}" ] && kill "$tunnel" 2>/dev/null
  ssh "$REMOTE" 'pkill -f "k6 run" 2>/dev/null' || true
  say "restore: aggregator 3, client-api $orig_api and its admission values"
  kn scale deploy/aggregator --replicas=3 >/dev/null || true
  kn set env deploy/client-api CLIENT_RATE_LIMIT_RPS- CLIENT_RATE_LIMIT_BURST- >/dev/null || true
  kn scale deploy/client-api --replicas="$orig_api" >/dev/null || true
  rm -rf "$work"
}
trap cleanup EXIT

say "test configuration: client-api x4 (admission 2500 req/s per pod, Media still shed first); aggregator 3"
kn set env deploy/client-api CLIENT_RATE_LIMIT_RPS=2500 CLIENT_RATE_LIMIT_BURST=5000 >/dev/null
kn scale deploy/client-api --replicas=4 >/dev/null
kn scale deploy/aggregator --replicas=3 >/dev/null
kn rollout status deploy/client-api --timeout=240s
kn rollout status deploy/aggregator --timeout=240s
kn get pods -l 'app in (aggregator,client-api)' -o wide

say "reverse tunnel: $REMOTE 127.0.0.1:30080/30084 -> $TUNNEL_TARGET"
ssh -N -o ExitOnForwardFailure=yes -o ServerAliveInterval=15 \
  -R "127.0.0.1:30080:$TUNNEL_TARGET:30080" -R "127.0.0.1:30084:$TUNNEL_TARGET:30084" "$REMOTE" &
tunnel=$!
sleep 4
ssh "$REMOTE" 'curl -s -o /dev/null -w "remote -> tunnel -> cluster: HTTP %{http_code}\n" http://127.0.0.1:30080/health'

common="K6_VUS=$half K6_RAMP=$K6_RAMP K6_HOLD=$K6_HOLD K6_THINK_S=$K6_THINK_S K6_MODE=scale K6_MEDIA_SHARE=0.9"
creds="K6_CLIENT_ID=$AUTH_LAPANGAN_CLIENT_ID K6_CLIENT_SECRET=$AUTH_LAPANGAN_CLIENT_SECRET K6_MEDIA_ID=$AUTH_MEDIA_CLIENT_ID K6_MEDIA_SECRET=$AUTH_MEDIA_CLIENT_SECRET"
started=$(date -u +%s)
# remote half
ssh "$REMOTE" "ulimit -n 65535; cd /tmp && env $common $creds CLIENT_API_URLS=http://127.0.0.1:30080 AUTH_URL=http://127.0.0.1:30084 K6_SUMMARY=/tmp/k6-remote.json k6 run --quiet m2.js" >"$work/remote.out" 2>&1 &
rpid=$!
# local half
(cd "$ROOT" && env $common $creds CLIENT_API_URLS="$LOCAL_URLS" AUTH_URL="$LOCAL_AUTH" K6_SUMMARY=docs/evidence/m2/k6-m2-scale-local.json \
  k6 run --quiet scripts/loadtest/m2.js) >"$work/local.out" 2>&1 &
lpid=$!
at() { local t=$1; shift
  while [ $(($(date -u +%s) - started)) -lt "$t" ]; do kill -0 "$lpid" 2>/dev/null || return 0; sleep 1; done
  printf '[t+%3ss %s] %s\n' "$(($(date -u +%s) - started))" "$(date -u +%T)" "$*"; "$@"; }
( while kill -0 "$lpid" 2>/dev/null; do
    printf '[t+%3ss] aggregator ready %s/%s\n' "$(($(date -u +%s) - started))" \
      "$(kn get deploy aggregator -o jsonpath='{.status.readyReplicas}' --request-timeout=5s 2>/dev/null)" \
      "$(kn get deploy aggregator -o jsonpath='{.spec.replicas}' --request-timeout=5s 2>/dev/null)"
    sleep 10
  done ) &
at $((ramp_s + SCALE_AT_5)) kn scale deploy/aggregator --replicas=5
at $((ramp_s + SCALE_AT_2)) kn scale deploy/aggregator --replicas=2
wait "$lpid"; wait "$rpid"
ended=$(date -u +%s)
scp -q "$REMOTE:/tmp/k6-remote.json" "$ROOT/docs/evidence/m2/k6-m2-scale-remote.json"
say "k6 output"
tail -n 3 "$work/local.out" | sed 's/^/   local:  /'
tail -n 3 "$work/remote.out" | sed 's/^/   remote: /'

say "merged result (both generators)"
"$(command -v python3 || command -v python)" - "$ROOT/docs/evidence/m2/k6-m2-scale-local.json" "$ROOT/docs/evidence/m2/k6-m2-scale-remote.json" <<'PY' | tee "$work/merged"
import json, sys
a, b = (json.load(open(p)) for p in sys.argv[1:3])
req = a["requests"] + b["requests"]
err = a["errors"] + b["errors"]
out = {"vus_total": a["vus_max"] + b["vus_max"], "duration_s": max(a["duration_s"], b["duration_s"]),
       "requests": req, "ok_200": a["ok_200"] + b["ok_200"], "shed_429": a["shed_429"] + b["shed_429"],
       "errors": err, "error_rate": err / req if req else 1.0,
       "lapangan": {k: a["by_role"]["lapangan"][k] + b["by_role"]["lapangan"][k] for k in ("ok_200", "shed_429", "errors")},
       "media": {k: a["by_role"]["media"][k] + b["by_role"]["media"][k] for k in ("ok_200", "shed_429", "errors")},
       "p95_ms": {"local": a["latency_ms"]["p95"], "remote": b["latency_ms"]["p95"]}}
print(json.dumps(out, indent=2))
print("RATE", out["error_rate"], "VUS", out["vus_total"])
PY
rate=$(awk '/^RATE/{print $2}' "$work/merged") vus=$(awk '/^RATE/{print $4}' "$work/merged")
awk -v v="$vus" -v want="$K6_VUS" 'BEGIN{exit !(v >= want)}' || die "only $vus VUs reached (wanted $K6_VUS)"
awk -v r="$rate" 'BEGIN{exit !(r < 0.01)}' || die "error rate $rate >= 1 % outside controlled 429s"
printf 'PASS %s VUs, error rate %s < 1 %% outside controlled 429s, while scaling 3 -> 5 -> 2\n' "$vus" "$rate"

say "criterion 2 during the test window"
SINCE="$(((ended - started) / 60 + 2))m" bash "$ROOT/scripts/demo/m2-single-writer.sh"
