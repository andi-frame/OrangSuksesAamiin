#!/usr/bin/env bash
# Problem 5 - pub/sub fan-out: Kafka topic, independent consumers, catch-up (Orang 3 / stack).
#
# Proves:
#   - Events reach the broker; consumers in separate groups both process new events
#   - Stopping one consumer does not block the Aggregator (ingest continues)
#   - Restarted consumer receives the exact event published while it was stopped
#   - A third consumer joins the existing topic without changing the producer
#
# Prerequisite: make up (REPOSITORY_BACKEND=postgres, PUBLISHER_BACKEND=kafka in .env)
. "$(dirname "$0")/../lib.sh"
require_stack

PEMDA_CONTAINER="p5-pemda-demo"
cleanup() {
  docker compose start consumer-notifier >/dev/null 2>&1 || true
  docker rm -f "$PEMDA_CONTAINER" >/dev/null 2>&1 || true
}
trap cleanup EXIT

DASH_URL="http://localhost:${CONSUMER_DASHBOARD_PORT:-8091}"
NOTIF_URL="http://localhost:${CONSUMER_NOTIFIER_PORT:-8092}"

has_hazard_agg() {
  [ "$(code -H "X-Service-Key: ${AGGREGATOR_SERVICE_KEY}" "${AGG_URL}/internal/hazards/$1")" = "200" ]
}

notif_up() {
  [ "$(code "${NOTIF_URL}/health")" = "200" ]
}

consumer_logged_event() {
  local service="$1" event_id="$2"
  docker compose logs --no-log-prefix "$service" 2>/dev/null |
    jqx -e --arg event_id "$event_id" \
      'select(.msg == "consumer_event" and .event_id == $event_id)' >/dev/null
}

producer_logged_event() {
  local event_id="$1"
  docker compose logs --no-log-prefix aggregator 2>/dev/null |
    jqx -e --arg event_id "$event_id" \
      'select(.msg == "outbox_published" and .event_id == $event_id)' >/dev/null
}

pemda_up() {
  # -O - plus a host-side redirect: Git Bash rewrites a literal /dev/null argument to "nul"
  docker exec "$PEMDA_CONTAINER" wget -q -O - http://127.0.0.1:8092/health >/dev/null
}

pemda_logged_event() {
  local event_id="$1"
  docker logs "$PEMDA_CONTAINER" 2>/dev/null |
    jqx -e --arg event_id "$event_id" \
      'select(.msg == "consumer_event" and .event_id == $event_id)' >/dev/null
}

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
wait_until "broker acknowledges bmkg-$G:1" 30 producer_logged_event "bmkg-$G:1"
wait_until "dashboard logs bmkg-$G:1" 30 consumer_logged_event consumer-dashboard "bmkg-$G:1"
wait_until "notifier logs bmkg-$G:1" 30 consumer_logged_event consumer-notifier "bmkg-$G:1"
ok "dashboard processed=$(cons_processed "$DASH_URL") notifier processed=$(cons_processed "$NOTIF_URL")"

step "C. Notifier stopped — Aggregator still ingests; dashboard still receives events"
docker compose stop consumer-notifier >/dev/null 2>&1 || fail "could not stop notifier"
sleep 2
BEFORE_D2="$(cons_processed "$DASH_URL")"
G2="$(bmkg_admin /admin/generate '{"count":1}' | jqx -r '.generated[0].event_id')"
wait_until "BMKG $G2 ingested" 45 has_hazard_agg "bmkg-$G2"
wait_until "broker acknowledges bmkg-$G2:1" 90 producer_logged_event "bmkg-$G2:1"
ok "aggregator ingested bmkg-$G2 while notifier is down"
wait_consume "$DASH_URL" "$BEFORE_D2" 90
wait_until "dashboard logs bmkg-$G2:1" 30 consumer_logged_event consumer-dashboard "bmkg-$G2:1"
ok "dashboard still consuming (producer not blocked by dead consumer)"

step "D. Notifier restarts and catches up"
docker compose start consumer-notifier >/dev/null 2>&1 || fail "could not restart notifier"
wait_until "notifier /health" 60 notif_up
wait_until "notifier receives offline event bmkg-$G2:1" 120 consumer_logged_event consumer-notifier "bmkg-$G2:1"
ok "notifier caught up with the exact offline event bmkg-$G2:1"

step "E. Add a new consumer group to the same topic without touching the producer"
AGG_ID="$(docker compose ps -q aggregator)"
AGG_STARTED="$(started_at aggregator)"
docker rm -f "$PEMDA_CONTAINER" >/dev/null 2>&1 || true
docker compose run -d --no-deps --name "$PEMDA_CONTAINER" \
  -e CONSUMER_NAME=pemda -e KAFKA_GROUP=pemda-portal consumer-notifier >/dev/null 2>&1 || fail "could not start pemda consumer"
wait_until "new pemda consumer healthy" 60 pemda_up
G3="$(bmkg_admin /admin/generate '{"count":1}' | jqx -r '.generated[0].event_id')"
wait_until "broker acknowledges bmkg-$G3:1" 90 producer_logged_event "bmkg-$G3:1"
wait_until "dashboard logs bmkg-$G3:1" 90 consumer_logged_event consumer-dashboard "bmkg-$G3:1"
wait_until "notifier logs bmkg-$G3:1" 90 consumer_logged_event consumer-notifier "bmkg-$G3:1"
wait_until "pemda logs bmkg-$G3:1" 90 pemda_logged_event "bmkg-$G3:1"
[ "$AGG_ID" = "$(docker compose ps -q aggregator)" ] || fail "producer container was replaced"
[ "$AGG_STARTED" = "$(started_at aggregator)" ] || fail "producer container was restarted"
ok "dashboard, notifier, and new pemda group received bmkg-$G3:1; producer was unchanged"

ok "P5 pub/sub demo complete"
