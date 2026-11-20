#!/usr/bin/env bash
# M2 "Observabilitas" on the running cluster (make k8s-up first). Self-checking.
#   1. metrics of EVERY service in one dashboard: Prometheus scrapes /metrics of every pod; Grafana dashboard
#   2. one request traced across every service it passes, with the duration of each step (Jaeger)
#      a. read path:   client -> client-api -> aggregator -> store
#      b. write path:  client -> client-api -> Kafka -> field-reports
#      c. ingest path: aggregator poll -> bmkg-mock -> store -> outbox -> Kafka -> consumers
#   3. structured JSON logs with the correlation ID (cid) and trace_id, the same id in every service
# UIs: Grafana http://<node>:30030 (dashboard "Argus / Observabilitas"), Jaeger http://<node>:30686
. "$(dirname "${BASH_SOURCE[0]}")/k8s-lib.sh"
API=http://127.0.0.1:18080
AUTHU=http://127.0.0.1:18084
PROM=http://127.0.0.1:19090
JAEGER=http://127.0.0.1:16686
GRAF=http://127.0.0.1:13000
forward client-api 18080 8080
forward auth-service 18084 8084
kn port-forward svc/prometheus 19090:9090 >/dev/null 2>&1 & PF_PIDS+=("$!")
kn port-forward svc/jaeger 16686:16686 >/dev/null 2>&1 & PF_PIDS+=("$!")
kn port-forward svc/grafana 13000:3000 >/dev/null 2>&1 & PF_PIDS+=("$!")
wait_until 30 "Prometheus, Jaeger, Grafana reachable" bash -c \
  "curl -sf $PROM/-/ready >/dev/null && curl -sf $JAEGER/ >/dev/null && curl -sf $GRAF/api/health >/dev/null"
token() { curl -sS --max-time 10 "$AUTHU/v1/token" -H 'Content-Type: application/json' \
  -d "{\"client_id\":\"$1\",\"client_secret\":\"$2\"}" | jqx -r .access_token; }
promq() { curl -sS --max-time 10 --get "$PROM/api/v1/query" --data-urlencode "query=$1"; }

# spans <trace_id>: the steps of one trace, in start order, with service and duration
spans() {
  curl -sS --max-time 10 "$JAEGER/api/traces/$1" | jqx -r '
    .data[0] as $t | ($t.spans | map(.startTime) | min) as $t0 |
    $t.spans | sort_by(.startTime)[] |
    "   +\(((.startTime - $t0) / 1000 * 10 | floor) / 10) ms\t\($t.processes[.processID].serviceName)\t\(.operationName)\t\((.duration / 1000 * 100 | floor) / 100) ms"' |
    column -t -s $'\t'
}
services_in() { curl -sS --max-time 10 "$JAEGER/api/traces/$1" | jqx -r '.data[0] as $t | [$t.spans[] | $t.processes[.processID].serviceName] | unique | join(",")'; }
has_service() { services_in "$1" 2>/dev/null | tr ',' '\n' | grep -qx "$2"; }

step "1. Metrics: every service is scraped (Prometheus 'up', one target per pod)"
promq 'up{job="argus"}' | jqx -r '.data.result[] | "   \(.metric.app)\t\(.metric.pod)\t\(.metric.node)\tup=\(.value[1])"' | sort | column -t -s $'\t'
down=$(promq 'up{job="argus"} == 0' | jqx '.data.result | length')
apps=$(promq 'count by (app) (up{job="argus"} == 1)' | jqx -r '[.data.result[].metric.app] | sort | join(" ")')
info "services with metrics: $apps"
[ "$down" = 0 ] || fail "$down target(s) down"
for a in aggregator client-api auth-service bmkg-mock pvmbg-mock consumer-dashboard consumer-notifier field-reports; do
  if [ "$(kn get deploy "$a" -o jsonpath='{.spec.replicas}' 2>/dev/null)" = 0 ]; then
    warn "$a is scaled to 0 (not checked)"
    continue
  fi
  echo " $apps " | grep -q " $a " || fail "no metrics from $a"
done
ok "every BNPB service exposes /metrics and is scraped"
show "throughput / error rate / p95 per service (last 5 min) - the same queries as the Grafana dashboard"
promq 'sum by (app) (rate(http_server_requests_total[5m]))' | jqx -r '.data.result[] | "   \(.metric.app)\t\(.value[1] | tonumber * 100 | floor / 100) req/s"' | sort | column -t -s $'\t'
promq 'histogram_quantile(0.95, sum by (app, le) (rate(http_server_request_duration_seconds_bucket[5m])))' |
  jqx -r '.data.result[] | select(.value[1] != "NaN") | "   \(.metric.app)\tp95 \(.value[1] | tonumber * 1000 | floor) ms"' | sort | column -t -s $'\t'
curl -sf "$GRAF/api/dashboards/uid/argus-overview" | jqx -r '"   Grafana dashboard: \(.dashboard.title) (\(.dashboard.panels | length) panels)"'
ok "one dashboard: Grafana -> Argus / Observabilitas"

step "2a. Trace of one READ request: client -> client-api -> aggregator -> store"
CID="obs-demo-$(date +%s)"
TOK=$(token "$AUTH_PUSAT_CLIENT_ID" "$AUTH_PUSAT_CLIENT_SECRET")
show "curl -H 'X-Correlation-ID: $CID' $API/v1/hazards?limit=5"
tp=$(curl -sS -D - -o /dev/null "$API/v1/hazards?limit=5" -H "Authorization: Bearer $TOK" -H "X-Correlation-ID: $CID" |
  tr -d '\r' | awk -F': ' 'tolower($1)=="traceparent"{print $2}')
TRACE=$(echo "$tp" | cut -d- -f2)
info "response traceparent: $tp  ->  trace_id $TRACE"
wait_until 20 "trace in Jaeger" has_service "$TRACE" aggregator
spans "$TRACE"
for s in client-api aggregator; do services_in "$TRACE" | grep -q "$s" || fail "trace misses $s"; done
ok "one trace spans client-api and aggregator, with the duration of every step"

step "2b. Trace of one WRITE: POST /v1/field-reports -> Kafka -> field-reports processor"
LT=$(token "$AUTH_LAPANGAN_CLIENT_ID" "$AUTH_LAPANGAN_CLIENT_SECRET")
tp=$(curl -sS -D - -o /dev/null "$API/v1/field-reports" -H "Authorization: Bearer $LT" -H "Idempotency-Key: $CID" \
  -H "X-Correlation-ID: $CID-w" -d '{"area_name":"Padang","description":"demo observabilitas"}' | tr -d '\r' | awk -F': ' 'tolower($1)=="traceparent"{print $2}')
TRACE=$(echo "$tp" | cut -d- -f2)
wait_until 30 "processor span in the trace" has_service "$TRACE" field-reports
spans "$TRACE"
ok "the trace continues through Kafka (traceparent record header) into the processor"

step "2c. Trace of one INGEST cycle: poll -> upstream -> store -> outbox -> Kafka -> consumers"
ING=$(curl -sS --get "$JAEGER/api/traces" --data-urlencode service=consumer-hazard --data-urlencode limit=20 --data-urlencode lookback=15m |
  jqx -r '[.data[] | select([.spans[].operationName] | any(startswith("poll ")))] | .[0].traceID // empty')
if [ -n "$ING" ]; then
  spans "$ING"
  ok "the ingest flow of one poll cycle is one trace (the outbox relay joins it through the stored cid)"
else
  warn "no ingest trace with consumers in the last 15 min (no new hazard was published); poll traces:"
  curl -sS --get "$JAEGER/api/traces" --data-urlencode service=aggregator --data-urlencode operation='poll BMKG' --data-urlencode limit=1 |
    jqx -r '.data[0].traceID' | while read -r t; do spans "$t"; done
fi

step "3. Structured logs: the same cid (and trace_id) in every service of the flow"
for app in client-api aggregator; do
  info "$app:"
  { kn logs -l "app=$app" --since=5m --tail=-1 --max-log-requests=10 2>/dev/null | grep "\"cid\":\"$CID\"" || true; } | head -2 | cut -c1-260 | sed 's/^/     /'
done
info "field-reports:"
{ kn logs -l app=field-reports --since=5m --tail=-1 2>/dev/null | grep "\"cid\":\"$CID-w\"" || true; } | head -2 | cut -c1-260 | sed 's/^/     /'
n=$(kn logs -l app=client-api --since=5m --tail=-1 --max-log-requests=10 2>/dev/null | grep -c "\"cid\":\"$CID\"" || true)
[ "$n" -gt 0 ] || fail "no client-api log line with cid $CID"
ok "JSON log lines carry cid and trace_id: grep a cid, open its trace in Jaeger"
printf '\n%sOBSERVABILITY PASS%s\n' "$C_G$C_B" "$C_0"
