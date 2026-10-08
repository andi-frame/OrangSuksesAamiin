#!/usr/bin/env bash
# Problem 4: independent deployment, dynamic JSONB attributes, and store isolation.
. "$(dirname "$0")/../lib.sh"
require_stack

sql() {
  docker compose exec -T postgres-canonical psql \
    -U "${CANONICAL_DB_USER:-canonical}" -d "${CANONICAL_DB_NAME:-canonical}" \
    -v ON_ERROR_STOP=1 -At -F '|' -c "$1"
}

hazard_exists() {
  [ "$(code -H "X-Service-Key: ${AGGREGATOR_SERVICE_KEY}" \
    "${AGG_URL}/internal/hazards/$1")" = 200 ]
}

pvmbg_ready() { [ "$(code "${PVMBG_URL}/health")" = 200 ]; }

bmkg_readable() {
  local token
  token="$(auth_token "$AUTH_LAPANGAN_CLIENT_ID" "$AUTH_LAPANGAN_CLIENT_SECRET" | jqx -r .access_token)"
  [ -n "$token" ] && [ "$(capi_code "$token" '/v1/hazards?hazard_type=SEISMIC&limit=1')" = 200 ]
}

container_snapshot() {
  local service
  for service in aggregator auth-service bmkg-mock client-api consumer-dashboard consumer-notifier \
    kafka postgres-canonical; do
    printf '%s|%s|%s\n' "$service" "$(docker compose ps -q "$service")" "$(started_at "$service")"
  done
}

cleanup() {
  docker compose start pvmbg-mock >/dev/null 2>&1 || true
  if pvmbg_ready; then
    pvmbg_admin /admin/schema-version '{"confidence_level":false,"extra_fields":{}}' >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

step "A. Rebuild only PVMBG while unrelated services keep serving requests"
BEFORE="$(container_snapshot)"
info "unrelated containers before rebuild:"
printf '%s\n' "$BEFORE" | sed 's/^/   /'
docker compose stop pvmbg-mock >/dev/null 2>&1 || fail "could not stop PVMBG"
bmkg_readable || fail "BMKG-only request failed while PVMBG was stopped"
docker compose build pvmbg-mock >/dev/null 2>&1 || fail "could not rebuild PVMBG"
bmkg_readable || fail "BMKG-only request failed while PVMBG was rebuilt"
docker compose up -d --no-deps pvmbg-mock >/dev/null 2>&1 || fail "could not restart PVMBG"
wait_until "PVMBG healthy after independent rebuild" 60 pvmbg_ready
AFTER="$(container_snapshot)"
[ "$BEFORE" = "$AFTER" ] || fail "an unrelated container was replaced or restarted"
info "unrelated containers after rebuild:"
printf '%s\n' "$AFTER" | sed 's/^/   /'
ok "BMKG-only requests succeeded; IDs and start times of all other containers stayed fixed"

step "B. Store an old-schema volcanic report"
pvmbg_admin /admin/schema-version '{"confidence_level":false,"extra_fields":{}}' >/dev/null
OLD_ID="$(pvmbg_admin /admin/generate '{"count":1}' | jqx -r '.generated[0].report_id')"
[[ "$OLD_ID" =~ ^[A-Za-z0-9_-]+$ ]] || fail "invalid generated report ID"
wait_until "old report pvmbg-$OLD_ID ingested" 60 hazard_exists "pvmbg-$OLD_ID"
OLD_ATTRS="$(sql "SELECT attributes::text FROM hazards WHERE hazard_id = 'pvmbg-$OLD_ID'")"
printf '%s' "$OLD_ATTRS" | jqx -e 'has("confidence_level") | not' >/dev/null || fail "old report has confidence_level"
SCHEMA_BEFORE="$(sql "SELECT md5(string_agg(column_name || ':' || data_type, ',' ORDER BY ordinal_position)) FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'hazards'")"
[[ "$SCHEMA_BEFORE" =~ ^[0-9a-f]{32}$ ]] || fail "could not fingerprint hazards columns"
AGG_STARTED="$(started_at aggregator)"
info "old row attributes: $OLD_ATTRS"
info "hazards table column fingerprint before: $SCHEMA_BEFORE"
ok "pvmbg-$OLD_ID stored without confidence_level in PostgreSQL JSONB"

step "C. Change PVMBG's schema live and store a new report without a migration"
pvmbg_admin /admin/schema-version '{"confidence_level":true}' >/dev/null
NEW_ID="$(pvmbg_admin /admin/generate '{"count":1}' | jqx -r '.generated[0].report_id')"
[[ "$NEW_ID" =~ ^[A-Za-z0-9_-]+$ ]] || fail "invalid generated report ID"
wait_until "new report pvmbg-$NEW_ID ingested" 60 hazard_exists "pvmbg-$NEW_ID"
NEW_ATTRS="$(sql "SELECT attributes::text FROM hazards WHERE hazard_id = 'pvmbg-$NEW_ID'")"
printf '%s' "$NEW_ATTRS" | jqx -e '.confidence_level | type == "number"' >/dev/null || fail "new report lacks confidence_level"
SCHEMA_AFTER="$(sql "SELECT md5(string_agg(column_name || ':' || data_type, ',' ORDER BY ordinal_position)) FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'hazards'")"
[ "$SCHEMA_BEFORE" = "$SCHEMA_AFTER" ] || fail "hazards table columns changed"
[ "$AGG_STARTED" = "$(started_at aggregator)" ] || fail "Aggregator restarted during schema change"
info "new row attributes: $NEW_ATTRS"
info "hazards table column fingerprint after: $SCHEMA_AFTER"
ok "pvmbg-$NEW_ID contains confidence_level; table columns and Aggregator start time did not change"

step "D. Read old and new records side by side through the Aggregator"
PAIR="$(sql "SELECT count(*) FILTER (WHERE NOT attributes ? 'confidence_level'), count(*) FILTER (WHERE attributes ? 'confidence_level') FROM hazards WHERE hazard_id IN ('pvmbg-$OLD_ID', 'pvmbg-$NEW_ID')")"
[ "$PAIR" = '1|1' ] || fail "old and new rows do not coexist in PostgreSQL ($PAIR)"
[ "$OLD_ATTRS" = "$(sql "SELECT attributes::text FROM hazards WHERE hazard_id = 'pvmbg-$OLD_ID'")" ] || fail "old row changed"
agg "/internal/hazards/pvmbg-$OLD_ID" | jqx -e '.attributes | has("confidence_level") | not' >/dev/null || fail "old row unreadable"
agg "/internal/hazards/pvmbg-$NEW_ID" | jqx -e '.attributes.confidence_level | type == "number"' >/dev/null || fail "new row unreadable"
ok "old=$OLD_ID and new=$NEW_ID coexist and are readable; old attributes are unchanged"

step "E. Prove only the Aggregator can access the Canonical Store"
bash scripts/demo/isolation.sh 2>&1 | sed 's/[[:space:]]*$//'
ok "P4 independent deployment, flexible storage, and access isolation complete"
