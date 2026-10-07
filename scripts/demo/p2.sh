#!/usr/bin/env bash
# Problem 2 - Client-Facing API (Orang 2): concurrency, availability, degradation.
#
#   A. PVMBG delay ~3s — BMKG-only via :8080 p95 < 300 ms
#   B. PVMBG outage (error) — volcanic data + stale markers; BMKG still updates
#   C. PVMBG outage (hang) — seismic reads stay fast
#   D. Recovery without container restart
#   E. k6: 50 VUs, 60s sustained (each VU logs in and renews its own token; 200/429 OK, unhandled < 1%)
#   F. k6 overload: 150 VUs, no think time -> excess shed with 429, no 5xx, service stays up
#
# Aggregator ingest side: make demo-p2-aggregator (Orang 1).
# Prerequisite: make up
. "$(dirname "$0")/../lib.sh"
require_client_stack

SEISMIC='/v1/hazards?hazard_type=SEISMIC&limit=20'
VOLCANIC='/v1/hazards?hazard_type=VOLCANIC&limit=50'

lapangan_token() {
  auth_token "${AUTH_LAPANGAN_CLIENT_ID}" "${AUTH_LAPANGAN_CLIENT_SECRET}" | jqx -r .access_token
}

pv_state_agg() {
  agg /internal/sources/status | jqx -r '.sources[] | select(.source=="PVMBG") | .state'
}

pv_is() { [ "$(pv_state_agg)" = "$1" ]; }

has_hazard() {
  [ "$(code -H "X-Service-Key: ${AGGREGATOR_SERVICE_KEY}" "${AGG_URL}/internal/hazards/$1")" = "200" ]
}

restore() {
  pvmbg_admin /admin/outage '{"enabled":false}' >/dev/null 2>&1 || true
  pvmbg_admin /admin/delay '{"min_ms":500,"max_ms":3000}' >/dev/null 2>&1 || true
}

# Access tokens live 60 s but this demo runs for minutes: T() prints a token that is at most 30 s old (renewed
# on demand), so no step measures its own token expiry instead of the system. The cache is a file because T is
# called inside command substitutions (subshells).
TOKFILE="$(mktemp)"
trap 'rm -f "$TOKFILE"; restore' EXIT
T() {
  local at tok now
  now="$(date +%s)"
  read -r at tok <"$TOKFILE" 2>/dev/null || true
  if [ -z "${tok:-}" ] || [ $((now - ${at:-0})) -ge 30 ]; then
    tok="$(lapangan_token)"
    [ -n "$tok" ] && [ "$tok" != "null" ] || return 1
    echo "$now $tok" >"$TOKFILE"
  fi
  printf '%s' "$tok"
}
[ -n "$(T)" ] || fail "could not obtain lapangan token"

capi_sees() {
  [ "$(capi_code "$(T)" "/v1/hazards/$1")" = "200" ]
}

STARTED_BEFORE="$(for s in bmkg-mock pvmbg-mock aggregator client-api auth-service; do echo "$s=$(started_at "$s")"; done)"
pvmbg_admin /admin/outage '{"enabled":false}' >/dev/null
wait_until "PVMBG source up" 60 pv_is up

step "A. PVMBG slow (3s) — BMKG-only via Client-Facing API p95 < 300 ms"
show "POST ${PVMBG_URL}/admin/delay {ms:3000}"
pvmbg_admin /admin/delay '{"ms":3000}' | jqx -c '{delay_min_ms,delay_max_ms}'
# the requests that need PVMBG data run CONCURRENTLY with the BMKG-only ones (4 parallel readers of the volcanic view)
VOLFILE="$(mktemp)"
VT="$(T)"
for i in 1 2 3 4; do capi_p95_ms 40 "$VT" "$VOLCANIC" >"$VOLFILE.$i" & done
read -r p95 bad <<<"$(capi_p95_ms 40 "$(T)" "$SEISMIC")"
wait
info "client-api seismic, 40 requests while PVMBG poll is slow and 4 readers fetch volcanic data in parallel: p95=${p95} ms, hard errors (not 200/429)=${bad}"
info "concurrent volcanic readers (p95 ms, hard errors): $(cat "$VOLFILE".{1,2,3,4} | paste -sd';' -)"
cat "$VOLFILE".{1,2,3,4} | awk '$2 != 0 { b = 1 } END { exit b }' || fail "volcanic readers got hard errors while PVMBG is slow"
rm -f "$VOLFILE" "$VOLFILE".{1,2,3,4}
awk "BEGIN{exit !($p95 < 300)}" && [ "$bad" = "0" ] \
  && ok "p95 ${p95} ms < 300 ms (BMKG-only through :8080)" \
  || fail "BMKG-only p95 ${p95} ms or errors ${bad}"

G="$(bmkg_admin /admin/generate '{"count":1}' | jqx -r '.generated[0].event_id')"
wait_until "BMKG event $G visible to client" 45 capi_sees "bmkg-$G"
ok "new BMKG event reachable via client-api while PVMBG is slow"

step "B. PVMBG outage (error) — stale data, no hard errors, BMKG still fresh"
BEFORE_VOL="$(capi "$(T)" "$VOLCANIC" | jqx '.items | length')"
show "POST ${PVMBG_URL}/admin/outage {enabled:true, mode:error}"
pvmbg_admin /admin/outage '{"enabled":true,"mode":"error"}' | jqx -c '{outage,outage_mode}'
wait_until "PVMBG down in aggregator status" 60 pv_is down

VBODY="$(capi "$(T)" "$VOLCANIC")"
[ "$(echo "$VBODY" | jqx '.items | length')" = "$BEFORE_VOL" ] \
  && ok "volcanic count unchanged ($BEFORE_VOL) during outage" \
  || fail "volcanic data lost during outage"

echo "$VBODY" | jqx -e '.freshness.PVMBG.stale_since != null' >/dev/null \
  && ok "freshness.PVMBG.stale_since set on client-api list" || fail "missing freshness.PVMBG.stale_since"

PV_N="$(echo "$VBODY" | jqx '[.items[] | select(.source=="PVMBG")] | length')"
if [ "$PV_N" -gt 0 ]; then
  echo "$VBODY" | jqx -e '[.items[] | select(.source=="PVMBG") | .stale] | all(. == true)' >/dev/null \
    && ok "all $PV_N PVMBG items have stale=true" || fail "PVMBG items must be stale during outage"
else
  ok "no PVMBG rows in this page (skip per-item stale check)"
fi

read -r p95 bad <<<"$(capi_p95_ms 25 "$(T)" "$VOLCANIC")"
awk "BEGIN{exit !($p95 < 300)}" && [ "$bad" = "0" ] \
  && ok "volcanic reads during outage: p95 ${p95} ms, no hard errors" \
  || fail "volcanic reads failed/slow: p95 ${p95} ms errors ${bad}"

G2="$(bmkg_admin /admin/generate '{"count":1}' | jqx -r '.generated[0].event_id')"
wait_until "BMKG $G2 during PVMBG outage" 45 capi_sees "bmkg-$G2"
ok "seismic path still works during PVMBG outage"

step "C. PVMBG outage (hang) — client-api stays responsive on seismic"
pvmbg_admin /admin/outage '{"enabled":true,"mode":"hang"}' | jqx -c '{outage,outage_mode}'
sleep 12
read -r p95 bad <<<"$(capi_p95_ms 25 "$(T)" "$SEISMIC")"
awk "BEGIN{exit !($p95 < 300)}" && [ "$bad" = "0" ] \
  && ok "seismic p95 ${p95} ms while PVMBG hangs" \
  || fail "client-api hung or slow during hang outage: ${p95} ms / ${bad}"

step "D. Recovery — PVMBG up again, no container restart"
show "POST ${PVMBG_URL}/admin/outage {enabled:false}"
pvmbg_admin /admin/outage '{"enabled":false}' >/dev/null
pvmbg_admin /admin/delay '{"min_ms":500,"max_ms":3000}' >/dev/null
wait_until "PVMBG up again" 90 pv_is up
ok "PVMBG source up again"

R="$(pvmbg_admin /admin/generate '{"count":1}' | jqx -r '.generated[0].report_id')"
wait_until "PVMBG report $R after recovery" 60 has_hazard "pvmbg-$R"
ok "new volcanic report ingested after recovery"

STARTED_AFTER="$(for s in bmkg-mock pvmbg-mock aggregator client-api auth-service; do echo "$s=$(started_at "$s")"; done)"
[ "$STARTED_BEFORE" = "$STARTED_AFTER" ] \
  && ok "no service container restarted during demo" \
  || fail "a container restarted — compare StartedAt lists"

step "E. Load test k6 (50 VUs, 60s) - every VU logs in and renews its own token"
if bash "$(dirname "$0")/../loadtest/run-p2.sh"; then
  ok "k6 load test passed: unhandled error rate < 1% and p95 < 300 ms (docs/evidence/p2/k6-summary.json)"
else
  fail "k6 load test failed (threshold crossed, or k6/docker missing) - see docs/evidence/p2/k6.log"
fi
bash "$(dirname "$0")/../report/sync-p2-metrics.sh" || true
S1=docs/evidence/p2/k6-summary.json
info "sustained run: $(jqx -c '{requests,throughput_rps,latency_ms,status,unhandled_error_rate,token_renewals}' <"$S1")"

step "F. Overload: 150 VUs with no think time for 20 s - excess is shed with 429, never 5xx, service stays up"
if K6_VUS=150 K6_THINK_S=0 K6_DURATION=20s K6_SUMMARY=docs/evidence/p2/k6-overload-summary.json \
   K6_LOG=docs/evidence/p2/k6-overload.log bash "$(dirname "$0")/../loadtest/run-p2.sh"; then
  S2=docs/evidence/p2/k6-overload-summary.json
  info "overload: $(jqx -c '{requests,throughput_rps,latency_ms,status,rate_429,unhandled_error_rate}' <"$S2")"
  jqx -e '.status.rejected_429 > 0' <"$S2" >/dev/null \
    && ok "controlled rejections happened and are reported (HTTP 429 count above)" \
    || fail "overload produced no 429: the limiter never engaged"
  jqx -e '.status.other == 0' <"$S2" >/dev/null \
    && ok "no 5xx / auth errors / timeouts under overload (only 200 and 429)" \
    || fail "overload produced hard errors"
  [ "$(code "${CAPI_URL}/health")" = "200" ] \
    && ok "client-api still answers /health right after the overload" \
    || fail "client-api stopped answering after overload"
else
  fail "overload run failed - see docs/evidence/p2/k6-overload.log"
fi

ok "P2 client-api demo complete"
