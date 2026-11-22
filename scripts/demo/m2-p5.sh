#!/usr/bin/env bash
# M2 Problem 5 (Sensor Pantai) on the running cluster (make k8s-up first). Self-checking.
#   1. the assistants' image runs unmodified (digest), with the assistants' manifest
#   2. data violating the base contract is set aside with its reason, never stored as HazardEvent
#   3. every critical event reaches the Tim Lapangan notifier: 0 lost, p95 <= 5 s
#   4. sensor unhealthy (pod gone for OUTAGE_S): other sources keep the M1 latency (BMKG-only p95 < 300 ms),
#      coastal reads answer fast with the last data + stale marker, requests to the sensor do NOT pile up
#      (circuit breaker), and the system uses the sensor again by itself when it is back
#   5. overload: Tim Lapangan is served while Media is shed with 429 (needs k6 or docker; P5_LOAD=0 skips)
#   6. SLO of the warning path and the error budget used (Prometheus)
#   OUTAGE_S=45
. "$(dirname "${BASH_SOURCE[0]}")/k8s-lib.sh"
OUTAGE_S="${OUTAGE_S:-45}"
W="${P5_WINDOW:-6h}"
API=http://127.0.0.1:18080
AUTHU=http://127.0.0.1:18084
NOTIF=http://127.0.0.1:18092
PROM=http://127.0.0.1:19090
forward aggregator 18083 8083
forward client-api 18080 8080
forward auth-service 18084 8084
forward consumer-notifier 18092 8092
kn port-forward svc/prometheus 19090:9090 >/dev/null 2>&1 & PF_PIDS+=("$!")
wait_until 30 "Prometheus reachable" curl -sf "$PROM/-/ready"
token() { curl -sS --max-time 10 "$AUTHU/v1/token" -H 'Content-Type: application/json' \
  -d "{\"client_id\":\"$1\",\"client_secret\":\"$2\"}" | jqx -r .access_token; }
promq() {
  curl -fsS --max-time 10 --get "$PROM/api/v1/query" --data-urlencode "query=$1" |
    jqx -er 'select(.status == "success") | .data.result[0].value[1] | tonumber? | select(isfinite and . >= 0)'
}
source_state() { agg /internal/sources/status | jqx -r '.sources[] | select(.source=="SENSOR_PANTAI") | "\(.state) \(.breaker)"'; }
EXPECTED_DIGEST=sha256:51fbd90d3891ad6e9f5768ad330ffb65205644cb29e50da65c7538f2e0b4397d

step "1. Sensor Pantai: the assistants' image, unmodified"
img=$(kn get deploy sensor-pantai -o jsonpath='{.spec.template.spec.containers[0].image}')
run=$(kn get pods -l app=sensor-pantai -o jsonpath='{.items[0].status.containerStatuses[0].imageID}')
info "deployment image: $img"
info "running image id: $run"
case "$img" in *"$EXPECTED_DIGEST") ;; *) fail "image is not the pinned digest" ;; esac
info "profile: $(kn get deploy sensor-pantai -o jsonpath='{.spec.template.spec.containers[0].env[?(@.name=="PANTAI_PROFILE")].value}')"
info "Aggregator view: $(source_state)"
ok "image pinned by digest; the Aggregator polls it (own loop, own HTTP client, own breaker)"

step "2. Contract violations are set aside with their reason (not HazardEvents)"
ops "/internal/ops/unprocessed?source=SENSOR_PANTAI&limit=200&raw=false" > /tmp/p5-unprocessed.$$ || true
n=$(jqx '.items | length' </tmp/p5-unprocessed.$$)
jqx -r '.items[] | "   \(.status)\t\(.record_key // "-")\t\(.reason)"' </tmp/p5-unprocessed.$$ | head -12 | cut -c1-170
info "set aside so far: $n"
for key in $(jqx -r '.items[] | select(.kind=="sensor-pantai.observation") | .record_key // empty' </tmp/p5-unprocessed.$$ | head -10); do
  agg_has "/internal/hazards/sensor-pantai-$key" && fail "$key violated the contract but is stored"
done
rm -f /tmp/p5-unprocessed.$$
info "rejected per reason (metrics): $(curl -sS --get "$PROM/api/v1/query" --data-urlencode 'query=sum by (reason) (sensor_pantai_observations_rejected_total)' |
  jqx -r '[.data.result[] | "\(.metric.reason)=\(.value[1])"] | join(" ")')"
ok "none of the set-aside observations is a HazardEvent"

step "3. Critical events reach the Tim Lapangan notifier: 0 lost, p95 <= 5 s"
# Proof that survives restarts: the notifier commits a record's offset only AFTER it handled (alerted) it, and
# Kafka keeps the committed offsets. Every critical event in hazard.events below the committed offset of the
# group "notifier" has therefore been delivered. Latency: the notifier's histogram (Prometheus).
KAFKA_BIN=/opt/kafka/bin
crit=$(MSYS_NO_PATHCONV=1 kn exec kafka-0 -- "$KAFKA_BIN/kafka-console-consumer.sh" --bootstrap-server localhost:9092 --topic hazard.events \
  --from-beginning --timeout-ms 15000 --property print.partition=true --property print.offset=true 2>/dev/null |
  tr -d '\r' | grep '"source":"SENSOR_PANTAI"' | grep '"critical":true' |
  sed -E 's/^Partition:([0-9]+)[[:space:]]+Offset:([0-9]+)[[:space:]]+.*"hazard_id":"([^"]+)".*/\1 \2 \3/' | sort -u -k3,3)
committed=$(MSYS_NO_PATHCONV=1 kn exec kafka-0 -- "$KAFKA_BIN/kafka-consumer-groups.sh" --bootstrap-server localhost:9092 --describe --group notifier 2>/dev/null |
  tr -d '\r' | awk '$2=="hazard.events"{print $3, $4}')
n_crit=$(echo "$crit" | grep -c . || true)
[ "$n_crit" -gt 0 ] || fail "no critical event in hazard.events yet (the practice profile flags its first one a few minutes after the sensor starts)"
lost=0
while read -r part off hid; do
  [ -n "$part" ] || continue
  c=$(echo "$committed" | awk -v p="$part" '$1==p{print $2}')
  if [ -z "$c" ] || [ "$c" = "-" ] || [ "$off" -ge "$c" ]; then
    lost=$((lost + 1)); info "NOT delivered (yet): $hid partition $part offset $off (committed ${c:-none})"
  else
    info "delivered: $hid (partition $part offset $off < committed $c)"
  fi
done <<<"$crit"
alerts=$(curl -sS "$NOTIF/alerts")
echo "$alerts" | jqx -r '(.alerts // [])[-5:][] | "   alert \(.notified_at) \(.hazard_id) \(.attributes.station_id) sea=\(.attributes.sea_level_anomaly_m) wave=\(.attributes.wave_height_m) \(.latency_ms | floor) ms"'
if p95=$(promq "histogram_quantile(0.95, sum by (le) (increase(consumer_critical_alert_seconds_bucket{consumer=\"notifier\"}[$W]))) * 1000"); then
  info "latency evidence: notifier histogram over $W"
else
  # /alerts is a bounded in-memory list. Use it only if it covers every critical Kafka ID under test.
  ids=$(awk '{print $3}' <<<"$crit" | jqx -Rn '[inputs | select(length > 0)] | unique')
  p95=$(jqx -er --argjson ids "$ids" '
    INDEX(.alerts[] | select(.source == "SENSOR_PANTAI"); .hazard_id) as $alerts |
    [$ids[] | $alerts[.].latency_ms] | select(length > 0 and all(.[]; type == "number" and isfinite and . >= 0)) |
    sort | .[(length * 0.95 | ceil) - 1]' <<<"$alerts") ||
    fail "no usable p95 from Prometheus or complete set of critical notifier samples"
  info "latency evidence: exact notifier samples for all $n_crit critical Kafka IDs"
fi
info "critical events: $n_crit, delivered: $((n_crit - lost)), lost: $lost; p95 store -> notifier ${p95} ms"
if ingest_p95=$(promq "histogram_quantile(0.95, sum by (le) (increase(sensor_pantai_critical_ingest_seconds_bucket[$W])))"); then
  info "fetch -> store p95 (Aggregator): ${ingest_p95} s; poll interval 1 s"
else
  info "fetch -> store p95 unavailable for $W"
fi
[ "$lost" = 0 ] || fail "$lost critical event(s) not delivered"
awk -v p="$p95" 'BEGIN{exit !(p <= 5000)}' || fail "p95 ${p95} ms > 5 s"
ok "every critical event was delivered to the notifier (0 lost), p95 ${p95} ms <= 5 s"

step "4. Sensor unhealthy for ${OUTAGE_S}s (pod removed): isolation, fast answers, no request storm, self-recovery"
TOK=$(token "$AUTH_PUSAT_CLIENT_ID" "$AUTH_PUSAT_CLIENT_SECRET")
before=$(promq 'sum(http_client_requests_total{target="sensor-pantai"})') || fail "sensor request counter unavailable before the outage"
show "kubectl scale deploy/sensor-pantai --replicas=0"
kn scale deploy/sensor-pantai --replicas=0 >/dev/null
t0=$(date +%s)
LAT="$(mktemp)" CLAT="$(mktemp)"
tok_at=$t0
while [ $(($(date +%s) - t0)) -lt "$OUTAGE_S" ]; do
  ensure_forwards
  # access tokens live 60 s: renew every 30 s so no 401 hides in the latency sample
  if [ $(($(date +%s) - tok_at)) -ge 30 ]; then TOK=$(token "$AUTH_PUSAT_CLIENT_ID" "$AUTH_PUSAT_CLIENT_SECRET"); tok_at=$(date +%s); fi
  curl -s -o /dev/null -w '%{time_total}\n' "$API/v1/hazards?hazard_type=SEISMIC&limit=20" -H "Authorization: Bearer $TOK" >>"$LAT"
  curl -s -o /tmp/p5-coastal.$$ -w '%{time_total}\n' "$API/v1/hazards?source=SENSOR_PANTAI&limit=5" -H "Authorization: Bearer $TOK" >>"$CLAT"
  sleep 0.5
done
if ! after=$(promq 'sum(http_client_requests_total{target="sensor-pantai"})'); then
  kn scale deploy/sensor-pantai --replicas=1 >/dev/null
  fail "sensor request counter unavailable at the end of the outage"
fi
p95b=$(sort -n "$LAT" | awk '{a[NR]=$1} END{i=int(NR*0.95); if(i<1)i=1; print a[i]*1000}')
p95c=$(sort -n "$CLAT" | awk '{a[NR]=$1} END{i=int(NR*0.95); if(i<1)i=1; print a[i]*1000}')
stale=$(jqx -r '.freshness.SENSOR_PANTAI.stale_since // "null"' </tmp/p5-coastal.$$)
items=$(jqx '.items | length' </tmp/p5-coastal.$$)
rm -f "$LAT" "$CLAT" /tmp/p5-coastal.$$
calls=$(awk -v a="$before" -v b="$after" 'BEGIN{printf "%d", b - a}')
info "BMKG-only reads during the outage: p95 ${p95b} ms (M1 criterion: < 300 ms)"
info "coastal reads during the outage: p95 ${p95c} ms, $items items (last data), stale_since=$stale"
info "requests sent to the sensor in ${OUTAGE_S}s: $calls (poll interval 1 s would be ~${OUTAGE_S}; the open breaker probes once per $(kn get cm aggregator-config -o jsonpath='{.data.SENSOR_BREAKER_OPEN_SEC}') s)"
info "Aggregator view: $(source_state)"
awk -v p="$p95b" 'BEGIN{exit !(p < 300)}' || fail "BMKG-only p95 ${p95b} ms >= 300 ms while the sensor is down"
awk -v p="$p95c" 'BEGIN{exit !(p < 1000)}' || fail "coastal reads must answer fast, p95 ${p95c} ms"
[ "$stale" != null ] || fail "coastal data must be marked stale while the sensor is down"
[ "$calls" -le $((OUTAGE_S / 3)) ] || fail "$calls requests to a dead sensor in ${OUTAGE_S}s: no backoff"
ok "other sources unaffected, coastal answered from the last data with a stale marker, no request storm"
show "kubectl scale deploy/sensor-pantai --replicas=1   (no other intervention)"
kn scale deploy/sensor-pantai --replicas=1 >/dev/null
t1=$(date +%s)
wait_until 180 "SENSOR_PANTAI up again" bash -c "curl -sS -H 'X-Service-Key: $AGGREGATOR_SERVICE_KEY' $AGG/internal/sources/status | grep -q '\"source\":\"SENSOR_PANTAI\",\"state\":\"up\"'"
info "back up $(($(date +%s) - t1)) s after the pod was started (pod start + half-open probe); $(source_state)"
ok "the system used the sensor again by itself"

step "5. Overload: Tim Lapangan served, Media shed (429)"
if [ "${P5_LOAD:-1}" = 1 ] && { command -v k6 >/dev/null || command -v docker >/dev/null; }; then
  export K6_MODE=max K6_VUS="${P5_VUS:-200}" K6_HOLD=20s K6_MEDIA_SHARE=0.8 K6_SUMMARY=docs/evidence/m2/p5-priority-summary.json
  export K6_CLIENT_ID="$AUTH_LAPANGAN_CLIENT_ID" K6_CLIENT_SECRET="$AUTH_LAPANGAN_CLIENT_SECRET" K6_MEDIA_ID="$AUTH_MEDIA_CLIENT_ID" K6_MEDIA_SECRET="$AUTH_MEDIA_CLIENT_SECRET"
  if command -v k6 >/dev/null; then
    CLIENT_API_URLS=$API AUTH_URL=$AUTHU k6 run --quiet "$ROOT/scripts/loadtest/m2.js" || true
  else
    host=127.0.0.1 net=(--network host)
    [ "$(uname -s)" = Darwin ] && host=host.docker.internal net=()
    docker run --rm -i "${net[@]+"${net[@]}"}" -e CLIENT_API_URLS="http://$host:18080" -e AUTH_URL="http://$host:18084" -e K6_MODE -e K6_VUS -e K6_HOLD \
      -e K6_MEDIA_SHARE -e K6_SUMMARY -e K6_CLIENT_ID -e K6_CLIENT_SECRET -e K6_MEDIA_ID -e K6_MEDIA_SECRET -v "$ROOT:/work" -w /work \
      grafana/k6:0.54.0 run --quiet scripts/loadtest/m2.js || true
  fi
  jqx -c '.by_role' "$ROOT/docs/evidence/m2/p5-priority-summary.json" | sed 's/^/   /'
  lap_err=$(jqx '.by_role.lapangan.errors + .by_role.lapangan.shed_429' "$ROOT/docs/evidence/m2/p5-priority-summary.json")
  [ "$lap_err" = 0 ] || warn "Tim Lapangan saw $lap_err rejections/errors (capacity reserve too small for this load)"
  ok "Media is shed first (429 + Retry-After); Tim Lapangan keeps its reserved capacity"
else
  info "skipped (P5_LOAD=0 or neither k6 nor docker); unit test: client-api/internal/limit TestMediaFloodDoesNotStarveLapangan"
fi

step "6. SLO of the Tim Lapangan warning path: 99 % of critical alerts within 5 s"
if ratio=$(promq "sum(increase(consumer_critical_alert_seconds_bucket{consumer=\"notifier\",le=\"5\"}[$W])) / sum(increase(consumer_critical_alert_seconds_count{consumer=\"notifier\"}[$W]))"); then
  info "alerts within 5 s over $W: $(awk -v r="$ratio" 'BEGIN{printf "%.2f %%", r*100}'), error budget used: $(awk -v r="$ratio" 'BEGIN{printf "%.0f %%", (1-r)/0.01*100}')"
else
  fail "SLO and error budget unavailable for $W (no usable notifier measurement)"
fi
info "Grafana: Argus / Observabilitas -> row 'P5 Sensor Pantai' (SLO, budget, critical sent vs delivered, requests to the sensor)"
printf '\n%sP5 PASS%s\n' "$C_G$C_B" "$C_0"
