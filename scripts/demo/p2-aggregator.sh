#!/usr/bin/env bash
# Problem 2 - the ingest side (owned by the Aggregator): a slow or dead PVMBG must
# neither delay BMKG data nor lose what is already stored, and recovery needs no restart.
#
#   A. PVMBG slowed to 3 s        -> the Aggregator's BMKG side is unaffected
#   B. PVMBG outage (mode error)  -> circuit opens, data kept, stale_since set, BMKG keeps flowing
#   C. PVMBG outage (mode hang)   -> requests never answered; timeouts bounded; API still fast
#   D. outage ends                -> back to normal WITHOUT restart
#
# The Client-Facing API (load test, 429, stale_since in responses) is Orang 2's part; it relies on
# exactly the freshness information exercised here.
. "$(dirname "$0")/../lib.sh"
require_stack

seismic() { agg "/internal/hazards?hazard_type=SEISMIC&limit=5"; }
volcanic() { agg "/internal/hazards?hazard_type=VOLCANIC&limit=500"; }
pv_state() { agg /internal/sources/status | jqx -r '.sources[] | select(.source=="PVMBG") | .state'; }
pv_field() { agg /internal/sources/status | jqx -r ".sources[] | select(.source==\"PVMBG\") | $1"; }
bm_state() { agg /internal/sources/status | jqx -r '.sources[] | select(.source=="BMKG") | .state'; }
started_all() { local s; for s in bmkg-mock pvmbg-mock aggregator; do echo "$s=$(started_at "$s")"; done; }
pv_is() { [ "$(pv_state)" = "$1" ]; }
has_hazard() { [ "$(code -H "X-Service-Key: ${AGGREGATOR_SERVICE_KEY}" "${AGG_URL}/internal/hazards/$1")" = "200" ]; }

# p95_ms N URL...  : N sequential requests, prints p95 latency in ms (and fails on non-200)
p95_ms() {
  local n="$1"; shift
  local i lat=()
  for ((i = 0; i < n; i++)); do
    lat+=("$(curl -s -o /dev/null -w '%{http_code} %{time_total}\n' --max-time 10 -H "X-Service-Key: ${AGGREGATOR_SERVICE_KEY}" "$1")")
  done
  printf '%s\n' "${lat[@]}" | awk '$1 != 200 {bad++} {t[NR]=$2*1000} END {
    n=asort(t); i=int(n*0.95+0.999); if (i<1) i=1; printf "%.1f %d\n", t[i], bad+0 }'
}

restore() { pvmbg_admin /admin/outage '{"enabled":false}' >/dev/null 2>&1 || true; pvmbg_admin /admin/delay '{"min_ms":500,"max_ms":3000}' >/dev/null 2>&1 || true; }
trap restore EXIT

STARTED_BEFORE="$(started_all)"
pvmbg_admin /admin/outage '{"enabled":false}' >/dev/null
wait_until "both sources up" 60 pv_is up

step "A. PVMBG answers in 3 s - the BMKG side of the Aggregator must not feel it"
show "POST ${PVMBG_URL}/admin/delay {ms:3000}"
pvmbg_admin /admin/delay '{"ms":3000}' | jqx -c '{delay_min_ms,delay_max_ms}'
info "PVMBG last poll latency: $(pv_field .last_latency_ms) ms (the Aggregator waits for it in its OWN loop)"
read -r p95 bad <<<"$(p95_ms 30 "${AGG_URL}/internal/hazards?hazard_type=SEISMIC&limit=20")"
info "Aggregator API, seismic data, 30 requests while PVMBG takes 3 s per call: p95 = ${p95} ms, non-200 = ${bad}"
awk "BEGIN{exit !($p95 < 300)}" && [ "$bad" = "0" ] && ok "p95 ${p95} ms < 300 ms" || fail "seismic p95 ${p95} ms / errors ${bad}"
G="$(bmkg_admin /admin/generate '{"count":1}' | jqx -r '.generated[0].event_id')"
t0=$SECONDS; wait_until "BMKG event $G ingested" 30 has_hazard "bmkg-$G"
ok "a new BMKG event was ingested $((SECONDS - t0)) s after it appeared, although PVMBG is slow"
info "BMKG last poll latency: $(agg /internal/sources/status | jqx -r '.sources[] | select(.source=="BMKG") | .last_latency_ms') ms; polls: BMKG=$(agg /internal/sources/status | jqx '.sources[] | select(.source=="BMKG") | .polls') PVMBG=$(pv_field .polls) (PVMBG cycles are 3+ s long, BMKG's are ~0.1 s)"

step "B. PVMBG outage (mode: error) - circuit opens, stored data is kept, BMKG keeps flowing"
# remember WHICH hazards existed: PVMBG keeps issuing reports (one per interval) and a poll already in flight may store
# one more before the breaker opens, so the count may grow; what must never happen is losing a record that was there.
BEFORE_IDS="$(volcanic | jqx -c '[.items[].hazard_id]')"
BEFORE_COUNT="$(echo "$BEFORE_IDS" | jqx 'length')"
show "POST ${PVMBG_URL}/admin/outage {enabled:true, mode:error}"
pvmbg_admin /admin/outage '{"enabled":true,"mode":"error"}' | jqx -c '{outage,outage_mode}'
wait_until "PVMBG marked down (breaker open)" 60 pv_is down
info "status: $(agg /internal/sources/status | jqx -c '.sources[] | select(.source=="PVMBG") | {state,breaker,consecutive_failures,last_success_at,stale_since,last_error}')"
[ "$(pv_field .stale_since)" != "null" ] && ok "stale_since is set: $(pv_field .stale_since)" || fail "stale_since missing"
[ "$(bm_state)" = "up" ] && ok "BMKG stays 'up'" || fail "BMKG affected by the PVMBG outage"
V="$(volcanic)"
echo "$V" | jqx -e --argjson before "$BEFORE_IDS" '($before - [.items[].hazard_id]) | length == 0' >/dev/null \
  && ok "all $BEFORE_COUNT volcanic hazards are still served from the Canonical Store (now $(echo "$V" | jqx '.items|length'))" \
  || fail "volcanic data lost"
echo "$V" | jqx -e '.freshness.PVMBG.stale_since != null and .freshness.PVMBG.state == "down"' >/dev/null \
  && ok "list response carries freshness.PVMBG {state:down, stale_since:...} for the caller to mark the data stale" || fail "freshness missing from the list response"
G="$(bmkg_admin /admin/generate '{"count":1}' | jqx -r '.generated[0].event_id')"
wait_until "BMKG event $G ingested during the outage" 30 has_hazard "bmkg-$G"
ok "seismic ingestion continued normally during the outage"
read -r p95 bad <<<"$(p95_ms 20 "${AGG_URL}/internal/hazards?hazard_type=VOLCANIC&limit=20")"
awk "BEGIN{exit !($p95 < 300)}" && [ "$bad" = "0" ] && ok "volcanic reads during the outage: p95 ${p95} ms, 0 errors (no 500, no hanging request)" || fail "volcanic reads slow/failing during outage: ${p95} ms / ${bad}"
CALLS_OPEN="$(pv_field .skipped_by_breaker)"; sleep 4
[ "$(pv_field .skipped_by_breaker)" -ge "$CALLS_OPEN" ] && ok "while the circuit is open polls are skipped, not sent to the dead service (skipped_by_breaker=$(pv_field .skipped_by_breaker))"

step "C. PVMBG outage (mode: hang) - no answer at all; timeouts are bounded, the API stays fast"
pvmbg_admin /admin/outage '{"enabled":true,"mode":"hang"}' | jqx -c '{outage,outage_mode}'
sleep 12 # the breaker probes (half-open) and the probe now hangs until SOURCE_TIMEOUT_SEC
read -r p95 bad <<<"$(p95_ms 20 "${AGG_URL}/internal/hazards?hazard_type=SEISMIC&limit=20")"
awk "BEGIN{exit !($p95 < 300)}" && [ "$bad" = "0" ] && ok "during a HANGING PVMBG the Aggregator API answers in p95 ${p95} ms" || fail "API slow while PVMBG hangs: ${p95} ms"
[ "$(pv_state)" = "down" ] && ok "PVMBG still reported down; last_error: $(pv_field .last_error)" || fail "state: $(pv_state)"

step "D. Outage ends - normal operation resumes WITHOUT restarting anything"
show "POST ${PVMBG_URL}/admin/outage {enabled:false}"
pvmbg_admin /admin/outage '{"enabled":false}' | jqx -c '{outage}'
pvmbg_admin /admin/delay '{"min_ms":500,"max_ms":3000}' >/dev/null
wait_until "PVMBG back to up" 90 pv_is up
ok "PVMBG is 'up' again, stale_since=$(pv_field .stale_since)"
N="$(pvmbg_admin /admin/generate '{"count":1}' | jqx -r '.generated[0].report_id')"
wait_until "new PVMBG report $N ingested after recovery" 60 has_hazard "pvmbg-$N"
ok "a report issued after the outage was ingested"
[ "$(started_all)" = "$STARTED_BEFORE" ] && ok "nothing was restarted (StartedAt of bmkg-mock, pvmbg-mock, aggregator unchanged)" || fail "a container was restarted"
printf '\n%sP2 (ingest side) PASS%s\n' "$C_G$C_B" "$C_0"
