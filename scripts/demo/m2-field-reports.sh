#!/usr/bin/env bash
# M2 Problem 3 criterion 6 on the running cluster (make k8s-up first). Self-checking.
#   POST /v1/field-reports answers 202 quickly even when the processing downstream is slow or dead; the
#   receiving component may die at any moment after the 202; no report that got a 202 is lost, none is
#   processed twice.
#
#   1. downstream DEAD (processor scaled to 0): N reports -> every one 202, fast
#   2. retries of the same reports (same Idempotency-Key) -> same report_id, 202 again
#   3. the receiver dies right after its 202s: every Client-Facing API pod is killed (grace 0)
#   4. downstream comes back SLOW (300 ms per report) and is killed in the middle of a transaction
#   5. check: every report_id that got a 202 is processed exactly once (output topic, read_committed)
#   N=40 (reports per phase)
. "$(dirname "${BASH_SOURCE[0]}")/k8s-lib.sh"
N="${N:-40}"
API=http://127.0.0.1:18080
AUTHU=http://127.0.0.1:18084
FR=http://127.0.0.1:18093
forward client-api 18080 8080
forward auth-service 18084 8084

token() { curl -sS --max-time 10 "$AUTHU/v1/token" -H 'Content-Type: application/json' \
  -d "{\"client_id\":\"$AUTH_LAPANGAN_CLIENT_ID\",\"client_secret\":\"$AUTH_LAPANGAN_CLIENT_SECRET\"}" | jqx -r .access_token; }
RUN="demo-$(date +%s)"
ACCEPTED="$(mktemp)"
trap 'rm -f "$ACCEPTED"; cleanup_pf' EXIT

# send <key>: POST one report; prints "<code> <seconds> <report_id>"
send() {
  ensure_forwards
  local out
  out=$(curl -sS --max-time 10 -w '\n%{http_code} %{time_total}' "$API/v1/field-reports" \
    -H "Authorization: Bearer $TOK" -H "Idempotency-Key: $1" -H 'Content-Type: application/json' \
    -d "{\"area_name\":\"Padang\",\"description\":\"laporan $1\",\"hazard_type\":\"COASTAL\",\"latitude\":-0.95,\"longitude\":100.35}" || true)
  local body status
  body=$(echo "$out" | sed '$d')
  status=$(echo "$out" | tail -n 1)
  echo "$status $(echo "$body" | jqx -r '.report_id // "-"' 2>/dev/null || echo -)"
}

processed_topic() { # all committed records of the output topic: "<key>" per line
  kn exec kafka-0 -- /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
    --topic field-reports.processed --from-beginning --isolation-level read_committed --timeout-ms 10000 \
    --property print.key=true --property print.value=false 2>/dev/null | tr -d '\r' | grep "^fr-" || true
}

step "1. Downstream dead: the processor is scaled to 0, then $N reports are sent"
kn scale deploy/field-reports --replicas=0 >/dev/null
kn wait --for=delete pod -l app=field-reports --timeout=60s >/dev/null 2>&1 || true
TOK=$(token)
TIMES="$(mktemp)"
for i in $(seq 1 "$N"); do
  read -r code secs id <<<"$(send "$RUN-a-$i")"
  [ "$code" = 202 ] || fail "report $i: HTTP $code (downstream down must not matter)"
  echo "$id" >>"$ACCEPTED"
  echo "$secs" >>"$TIMES"
done
read -r p50 p95 pmax <<<"$(sort -n "$TIMES" | awk '{a[NR]=$1} END{i=int(NR*0.95); if(i<1)i=1; printf "%.3f %.3f %.3f", a[int((NR+1)/2)], a[i], a[NR]}')"
rm -f "$TIMES"
ok "$N x 202 Accepted while nothing processes them (curl time_total: p50 ${p50}s, p95 ${p95}s, max ${pmax}s)"
awk -v m="$p95" 'BEGIN{exit !(m < 1.0)}' || fail "202 must be fast: p95 ${p95}s >= 1 s"

step "2. Retries with the same Idempotency-Key (lost 202, double tap)"
TOK=$(token)
for i in 1 2 3 4 5; do
  read -r code _ id <<<"$(send "$RUN-a-$i")"
  [ "$code" = 202 ] || fail "retry $i: HTTP $code"
  grep -qx "$id" "$ACCEPTED" || fail "retry $i got another report_id ($id)"
done
ok "5 retries answered 202 with the SAME report_id (they will be skipped as duplicates)"

step "3. The receiver dies right after its 202s: $N more reports, then every client-api pod is killed (grace 0)"
TOK=$(token)
for i in $(seq 1 "$N"); do
  read -r code _ id <<<"$(send "$RUN-b-$i")"
  [ "$code" = 202 ] || fail "report b-$i: HTTP $code"
  echo "$id" >>"$ACCEPTED"
done
show "kubectl delete pod -l app=client-api --grace-period=0 --force"
kn delete pod -l app=client-api --grace-period=0 --force >/dev/null 2>&1 || true
kn rollout status deploy/client-api --timeout=180s >/dev/null
ok "client-api pods replaced; nothing was kept in their memory (the reports were in Kafka before each 202)"

step "4. Downstream back but SLOW (300 ms per report); killed in the middle of a transaction"
kn set env deploy/field-reports FIELD_REPORT_PROCESS_DELAY_MS=300 >/dev/null
kn scale deploy/field-reports --replicas=1 >/dev/null
kn rollout status deploy/field-reports --timeout=180s >/dev/null
forward field-reports 18093 8093
wait_until 60 "processor busy" bash -c "curl -s $FR/stats | grep -q '\"processed\":[1-9]'"
curl -s "$FR/stats" | sed 's/^/   before kill: /'
show "kubectl delete pod -l app=field-reports --grace-period=0 --force   (open transaction aborted)"
kn delete pod -l app=field-reports --grace-period=0 --force >/dev/null 2>&1 || true
kn set env deploy/field-reports FIELD_REPORT_PROCESS_DELAY_MS=0 >/dev/null
kn rollout status deploy/field-reports --timeout=180s >/dev/null
ensure_forwards

step "5. Every report that got a 202 is processed exactly once"
want=$(sort -u "$ACCEPTED" | wc -l | tr -d ' ')
check() {
  local got
  got=$(processed_topic | grep -cxFf <(sort -u "$ACCEPTED") || true)
  [ "$got" -ge "$want" ]
}
wait_until 180 "all $want accepted reports processed" check
topic=$(processed_topic)
total=$(echo "$topic" | grep -cxFf <(sort -u "$ACCEPTED") || true)
distinct=$(echo "$topic" | grep -xFf <(sort -u "$ACCEPTED") | sort -u | wc -l | tr -d ' ')
info "accepted (distinct report_id with a 202): $want"
info "in field-reports.processed (read_committed): $total records, $distinct distinct"
curl -s "$FR/stats" | sed 's/^/   processor now: /'
kn logs deploy/field-reports --since=10m | grep -E '"msg":"(transaction_aborted|dedup_set_rebuilt|field_report_duplicate_skipped)"' |
  jqx -r '"   \(.ts) \(.msg) \(.report_id // .known_reports // "")"' | tail -6
[ "$distinct" = "$want" ] || fail "lost reports: $want accepted, $distinct processed"
[ "$total" = "$want" ] || fail "processed twice: $total records for $want reports"
ok "no report lost, none processed twice ($want accepted -> $want processed, once each)"

step "6. Status for the reporter"
id=$(head -1 "$ACCEPTED")
show "GET /v1/field-reports/$id"
curl -sS "$API/v1/field-reports/$id" -H "Authorization: Bearer $(token)" | jqx -c '{report_id, status, received_at, processed_at, input}' | sed 's/^/   /'
printf '\n%sP3 CRITERION 6 PASS%s\n' "$C_G$C_B" "$C_0"
