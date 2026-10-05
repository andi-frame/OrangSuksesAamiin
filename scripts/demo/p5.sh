#!/usr/bin/env bash
# Problem 5 - pub/sub fan-out: Kafka topic, independent consumers, catch-up (Orang 3 / stack).
#
# Proves:
#   - Events reach the broker; consumers in separate groups both process new events
#   - Stopping one consumer does not block the Aggregator (ingest continues)
#   - Restarted consumer catches up (processed count increases)
#
# Prerequisite: make up (REPOSITORY_BACKEND=postgres, PUBLISHER_BACKEND=kafka in .env)
. "$(dirname "$0")/../lib.sh"
require_stack

DASH_URL="http://localhost:${CONSUMER_DASHBOARD_PORT:-8091}"
NOTIF_URL="http://localhost:${CONSUMER_NOTIFIER_PORT:-8092}"

cons_processed() {
  curl -sS --max-time 5 "$1/stats" | jqx -r .processed
}

wait_consume() {
  local url="$1" before="$2" timeout="$3"
  wait_until "consumer $url processed > $before" "$timeout" cons_grew "$url" "$before"
}

cons_grew() {
  local url="$1" before="$2"
  [ "$(cons_processed "$url")" -gt "$before" ]
}

step "A. Both consumers are healthy and subscribed"
[ "$(code "${DASH_URL}/health")" = "200" ] && ok "consumer-dashboard /health" || fail "dashboard down"
[ "$(code "${NOTIF_URL}/health")" = "200" ] && ok "consumer-notifier /health" || fail "notifier down"

step "B. New hazard event is published and received by BOTH consumers"
BEFORE_D="$(cons_processed "$DASH_URL")"
BEFORE_N="$(cons_processed "$NOTIF_URL")"
G="$(bmkg_admin /admin/generate '{"count":1}' | jqx -r '.generated[0].event_id')"
info "generated BMKG event $G — waiting for Kafka fan-out"
wait_consume "$DASH_URL" "$BEFORE_D" 90
wait_consume "$NOTIF_URL" "$BEFORE_N" 90
ok "dashboard processed=$(cons_processed "$DASH_URL") notifier processed=$(cons_processed "$NOTIF_URL")"

step "C. Notifier stopped — Aggregator still ingests; dashboard still receives events"
docker compose stop consumer-notifier >/dev/null
sleep 2
BEFORE_D2="$(cons_processed "$DASH_URL")"
G2="$(bmkg_admin /admin/generate '{"count":1}' | jqx -r '.generated[0].event_id')"
wait_until "BMKG $G2 ingested" 45 has_hazard_agg "bmkg-$G2"
ok "aggregator ingested bmkg-$G2 while notifier is down"
wait_consume "$DASH_URL" "$BEFORE_D2" 90
ok "dashboard still consuming (producer not blocked by dead consumer)"

step "D. Notifier restarts and catches up"
docker compose start consumer-notifier >/dev/null
wait_until "notifier /health" 60 notif_up
NOTIF_BEFORE="$(cons_processed "$NOTIF_URL")"
wait_consume "$NOTIF_URL" "$NOTIF_BEFORE" 120
ok "notifier caught up after restart processed=$(cons_processed "$NOTIF_URL")"

ok "P5 pub/sub demo complete"

has_hazard_agg() {
  [ "$(code -H "X-Service-Key: ${AGGREGATOR_SERVICE_KEY}" "${AGG_URL}/internal/hazards/$1")" = "200" ]
}

notif_up() {
  [ "$(code "${NOTIF_URL}/health")" = "200" ]
}
