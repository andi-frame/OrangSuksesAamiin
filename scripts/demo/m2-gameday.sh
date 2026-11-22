#!/usr/bin/env bash
# M2 Problem 5 criteria 6 and 8: game day. A combination of failures runs while NOBODY intervenes; the script only
# observes. Afterwards: did the system recover by itself, and how much of the warning path's error budget was used.
#
#   Timeline (seconds from start, all concurrent with a Media + Tim Lapangan load and a Tim Lapangan probe):
#     0     k6 load starts (GD_VUS VUs, 90 % Media), Tim Lapangan probe every 1 s
#     30    PVMBG outage (mock admin, mode hang = no answer) until t+95
#     45    Tester Agent practice scenario GD_SCENARIO_1 (default kill-leader)
#     90    Sensor Pantai pod killed (the Deployment restarts it)
#     150   Tester Agent practice scenario GD_SCENARIO_2 (default wipe-node: a worker and its data)
#   then a recovery window of GD_RECOVERY_S without any action, then the checks:
#     - every Deployment/StatefulSet ready again, storage redundant, one ingest leader, sources up
#     - Tim Lapangan probe: share answered 200 and p95 latency
#     - SLO (99 % of critical alerts within 5 s) and error budget used during the game day (Prometheus)
#   Env: GD_VUS (2000) GD_SCENARIO_1 GD_SCENARIO_2 GD_RECOVERY_S (300) SENSOR_EXAM_KEY (file: exam profile)
. "$(dirname "${BASH_SOURCE[0]}")/k8s-lib.sh"
export MSYS_NO_PATHCONV=1   # Git Bash: keep /tmp/patroni.yaml and /usr/local/bin paths for kubectl exec as they are
GD_VUS="${GD_VUS:-2000}" GD_SCENARIO_1="${GD_SCENARIO_1:-kill-leader}" GD_SCENARIO_2="${GD_SCENARIO_2:-wipe-node}"
GD_RECOVERY_S="${GD_RECOVERY_S:-300}"
API="${API:-http://127.0.0.1:30080}" AUTHU="${AUTHU:-http://127.0.0.1:30084}" PROM="${PROM:-http://127.0.0.1:30090}"
evidence="$ROOT/docs/evidence/m2/p5-gameday-latest.log"
work=$(mktemp -d)
trap 'for p in ${bg:-}; do kill "$p" 2>/dev/null || true; done; rm -rf "$work"; cleanup_pf' EXIT
exec > >(tee "$evidence") 2>&1
promq() { curl -sS --max-time 10 --get "$PROM/api/v1/query" --data-urlencode "query=$1" | jqx -r '.data.result[0].value[1] // "0"'; }
at() { local t=$1; shift; while [ $((SECONDS - t0)) -lt "$t" ]; do sleep 1; done; printf '[t+%3ss %s] %s\n' "$((SECONDS - t0))" "$(date -u +%T)" "$*"; "$@"; }

step "0. Before: system healthy, profile of Sensor Pantai"
[ -n "${SENSOR_EXAM_KEY:-}" ] && bash "$ROOT/scripts/k8s/sensor-exam.sh" exam "$SENSOR_EXAM_KEY"
kn get pods -o wide -L role --no-headers | awk '{printf "   %-40s %-6s %-8s %s\n", $1, $2, $3, $7}'
start=$(date -u +%s)
token=$(curl -sS --max-time 10 "$AUTHU/v1/token" -H 'Content-Type: application/json' \
  -d "{\"client_id\":\"$AUTH_LAPANGAN_CLIENT_ID\",\"client_secret\":\"$AUTH_LAPANGAN_CLIENT_SECRET\"}" | jqx -r .access_token)
[ -n "$token" ] && [ "$token" != null ] || fail "no Tim Lapangan token"

step "1. Game day: failures injected on a timeline, no intervention"
t0=$SECONDS
(while :; do
  curl -s -o /dev/null -w "%{http_code} %{time_total}\n" --max-time 5 -H "Authorization: Bearer $token" \
    "$API/v1/hazards?limit=20" >>"$work/probe" 2>/dev/null || echo "000 5" >>"$work/probe"
  sleep 1
done) & bg="$!"
if command -v k6 >/dev/null 2>&1; then
  (cd "$ROOT" && CLIENT_API_URLS="$API" AUTH_URL="$AUTHU" K6_VUS="$GD_VUS" K6_RAMP=20s K6_HOLD=240s K6_THINK_S=1 \
    K6_CLIENT_ID="$AUTH_LAPANGAN_CLIENT_ID" K6_CLIENT_SECRET="$AUTH_LAPANGAN_CLIENT_SECRET" \
    K6_MEDIA_ID="$AUTH_MEDIA_CLIENT_ID" K6_MEDIA_SECRET="$AUTH_MEDIA_CLIENT_SECRET" K6_MEDIA_SHARE=0.9 \
    K6_SUMMARY=docs/evidence/m2/k6-gameday-summary.json k6 run --quiet scripts/loadtest/m2.js >"$work/k6.out" 2>&1) &
  bg="$bg $!"
  info "k6: $GD_VUS VUs (90 % Media) for 260 s"
else
  warn "k6 not installed: game day without the load part"
fi
at 30 pvmbg_admin /admin/outage '{"enabled":true,"mode":"hang"}' >/dev/null
at 45 bash "$ROOT/scripts/k8s/practice.sh" "$GD_SCENARIO_1" >"$work/s1.log" 2>&1 &
bg="$bg $!"
at 90 kn delete pod -l app=sensor-pantai --wait=false
at 95 pvmbg_admin /admin/outage '{"enabled":false}' >/dev/null   # the mock comes back; the system must notice by itself
at 150 bash "$ROOT/scripts/k8s/practice.sh" "$GD_SCENARIO_2" >"$work/s2.log" 2>&1
info "scenarios done at t+$((SECONDS - t0))s; recovery window ${GD_RECOVERY_S}s without any action"
sleep "$GD_RECOVERY_S"
for p in $bg; do kill "$p" 2>/dev/null || true; done; bg=
end=$(date -u +%s)
tail -n 12 "$work/s1.log" | sed 's/^/   tester-1: /'
tail -n 12 "$work/s2.log" | sed 's/^/   tester-2: /'
[ -f "$work/k6.out" ] && sed 's/^/   k6: /' "$work/k6.out" | tail -n 3

step "2. Recovered by itself?"
notready=$(kn get deploy,statefulset -o jsonpath='{range .items[*]}{.metadata.name} {.status.readyReplicas}/{.spec.replicas}{"\n"}{end}' |
  awk '{split($2, r, "/"); if (r[1] + 0 < r[2] + 0) print}')
[ -z "$notready" ] || { echo "$notready" | sed 's/^/   /'; fail "not every component is ready again"; }
ok "every Deployment and StatefulSet ready again"
info "ingest leader: $(leader), sources: $(agg /internal/sources/status | jqx -c '[.sources[] | "\(.source)=\(.state)"]')"
kn get pods -l app=canonical-shard -o wide -L shard,db-role --no-headers | awk '{printf "   %-10s %-8s %-8s %s\n", $1, $(NF-1), $NF, $7}'

step "3. Tim Lapangan during the game day (probe every 1 s)"
awk '{n++; if ($1 == "200") okc++; t[n] = $2 + 0} END {asort(t); printf "   requests %d, 200: %d (%.2f %%), p95 %.3f s\n", n, okc, 100 * okc / n, t[int(n * 0.95)]}' "$work/probe"

step "4. SLO of the warning path: 99 % of critical alerts within 5 s - error budget used during the game day"
W="$((end - start))s"
ratio=$(promq "sum(increase(consumer_critical_alert_seconds_bucket{le=\"5\"}[$W])) / clamp_min(sum(increase(consumer_critical_alert_seconds_count[$W])), 1e-9)")
total=$(promq "sum(increase(consumer_critical_alert_seconds_count[$W]))")
sent=$(promq "sum(increase(sensor_pantai_critical_total[$W]))")
info "window $W: critical events from the sensor $(printf '%.0f' "$sent"), alerts delivered $(printf '%.0f' "$total")"
info "within 5 s: $(awk -v r="$ratio" 'BEGIN{printf "%.2f %%", r*100}'), error budget used: $(awk -v r="$ratio" -v n="$total" 'BEGIN{if (n == 0) print "n/a (no critical alert)"; else printf "%.0f %% of the 1 %% budget", (1-r)/0.01*100}')"
info "Grafana: Argus / Observabilitas, time range $(date -u -d "@$start" +%T 2>/dev/null || echo "$start")..$(date -u -d "@$end" +%T 2>/dev/null || echo "$end") UTC"
printf '\n%sGAME DAY DONE%s (post-mortem: docs/report/m2/05-p5.md §6)\n' "$C_G$C_B" "$C_0"
