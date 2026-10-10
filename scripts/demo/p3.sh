#!/usr/bin/env bash
# Problem 3 - client auth, role-based field access, access TTL + refresh (Orang 2).
#
# Proves:
#   - Client-Facing API: 401 without credentials or with a bad/expired Bearer; 403 only for a valid token that may not do it
#   - Media: summary-only responses; explicit Mentah request -> 403
#   - Lapangan/Pusat: Mentah allowed; /v1/sources/status for lapangan (media denied)
#   - Access token expires (~AUTH_ACCESS_TTL_SEC); refresh without re-login; old tokens rejected
#
# Agency credential isolation (BMKG vs PVMBG): make demo-mock-auth (Orang 1).
# Prerequisite: make up
. "$(dirname "$0")/../lib.sh"
require_client_stack

HZ='/v1/hazards?hazard_type=SEISMIC&limit=20'

lapangan_token() {
  auth_token "${AUTH_LAPANGAN_CLIENT_ID}" "${AUTH_LAPANGAN_CLIENT_SECRET}" | jqx -r .access_token
}

media_token() {
  auth_token "${AUTH_MEDIA_CLIENT_ID}" "${AUTH_MEDIA_CLIENT_SECRET}" | jqx -r .access_token
}

pusat_token() {
  auth_token "${AUTH_PUSAT_CLIENT_ID}" "${AUTH_PUSAT_CLIENT_SECRET}" | jqx -r .access_token
}

hazards_ready() {
  local tok
  tok="$(lapangan_token)" || return 1
  [ "$(capi "$tok" "$HZ" | jqx '.items | length')" -ge 1 ]
}

jwt_exp_epoch() {
  echo "$1" | jqx -R 'split(".")[1] | @base64d | fromjson | .exp'
}

mentah_present() {
  echo "$1" | jqx -e '.items[0] | has("latitude") or has("attributes") or has("source_ref_id")' >/dev/null
}

wait_until "at least one seismic hazard via lapangan" 90 hazards_ready

step "A. Client-Facing API rejects missing or invalid credentials"
show "GET ${CAPI_URL}${HZ} (no Authorization)"
[ "$(curl -s -o /dev/null -w '%{http_code}' --max-time 10 "${CAPI_URL}${HZ}")" = "401" ] \
  && ok "no header -> 401" || fail "expected 401 without Authorization"

show "GET with empty Bearer"
[ "$(curl -s -o /dev/null -w '%{http_code}' --max-time 10 -H 'Authorization: Bearer ' "${CAPI_URL}${HZ}")" = "401" ] \
  && ok "empty Bearer -> 401" || fail "expected 401 for empty bearer"

[ "$(capi_code 'not.a.valid.jwt' "$HZ")" = "401" ] && ok "garbage JWT -> 401" || fail "expected 401 for invalid token"

step "B. Media receives Ringkasan only (enforced on the server)"
MEDIA="$(media_token)"
[ -n "$MEDIA" ] && [ "$MEDIA" != "null" ] || fail "could not obtain media token"

BODY="$(capi "$MEDIA" "$HZ")"
[ "$(echo "$BODY" | jqx '.items | length')" -ge 1 ] || fail "need at least one hazard for media check"

for k in latitude longitude attributes source_ref_id; do
  echo "$BODY" | jqx -e ".items[0] | has(\"$k\") | not" >/dev/null \
    || fail "media item must not contain $k"
done
ok "media list omits all Mentah fields on first item"

step "C. Media cannot request Mentah explicitly"
[ "$(capi_code "$MEDIA" "${HZ}&fields=latitude")" = "403" ] \
  && ok "media ?fields=latitude -> 403" || fail "media must not request latitude"
[ "$(capi_code "$MEDIA" "${HZ}&include=mentah")" = "403" ] \
  && ok "media ?include=mentah -> 403" || fail "media must not request include=mentah"
# every spelling of the same request is refused (a whitelist, not a list of known Mentah names)
for q in "fields=Latitude" "fields=hazard_id,attributes" "fields%5B%5D=longitude" "fields=source_ref_id" \
         "include=MENTAH" "include=mentah,summary" "include=raw" "include=all"; do
  [ "$(capi_code "$MEDIA" "${HZ}&${q}")" = "403" ] \
    && ok "media ?${q} -> 403" || fail "media ?${q} must be refused with 403"
done
[ "$(capi_code "$MEDIA" "${HZ}&fields=hazard_id,severity&include=summary")" = "200" ] \
  && ok "media asking only for Ringkasan fields -> 200" || fail "media Ringkasan request must succeed"

step "D. Media denied /v1/sources/status; Lapangan allowed"
[ "$(capi_code "$MEDIA" /v1/sources/status)" = "403" ] \
  && ok "media /v1/sources/status -> 403" || fail "media must not read sources status"

LAP="$(lapangan_token)"
[ "$(capi_code "$LAP" /v1/sources/status)" = "200" ] \
  && ok "lapangan /v1/sources/status -> 200" || fail "lapangan should read sources status"

step "E. Lapangan and Pusat receive Mentah on the same list"
LAP_BODY="$(capi "$LAP" "$HZ")"
mentah_present "$LAP_BODY" && ok "lapangan list includes Mentah-capable payload" \
  || fail "lapangan should receive full hazard shape (Mentah fields when upstream has them)"

PUSAT="$(pusat_token)"
PUS_BODY="$(capi "$PUSAT" "$HZ")"
mentah_present "$PUS_BODY" && ok "pusat list includes Mentah-capable payload" \
  || fail "pusat should receive full hazard shape"

step "F. Access token TTL: expired access rejected; refresh without new client login"
PAIR="$(auth_token "${AUTH_LAPANGAN_CLIENT_ID}" "${AUTH_LAPANGAN_CLIENT_SECRET}")"
ACCESS="$(echo "$PAIR" | jqx -r .access_token)"
REFRESH="$(echo "$PAIR" | jqx -r .refresh_token)"
EXPIRES_IN="$(echo "$PAIR" | jqx -r .expires_in)"
info "expires_in=${EXPIRES_IN}s (configured AUTH_ACCESS_TTL_SEC=${AUTH_ACCESS_TTL_SEC:-60})"

[ "$(capi_code "$ACCESS" "$HZ")" = "200" ] && ok "fresh access token works" || fail "new access token must work"

EXP="$(jwt_exp_epoch "$ACCESS")"
NOW="$(date +%s)"
WAIT=$((EXP - NOW + 2))
if [ "$WAIT" -lt 2 ]; then WAIT=2; fi
if [ "$WAIT" -gt 125 ]; then
  fail "access TTL wait ${WAIT}s too long for demo — set AUTH_ACCESS_TTL_SEC=60 and restart auth-service"
fi
info "sleeping ${WAIT}s until JWT exp (+2s skew)..."
sleep "$WAIT"

[ "$(capi_code "$ACCESS" "$HZ")" = "401" ] \
  && ok "expired access token -> 401" || fail "expired access must be rejected (401)"

show "POST ${AUTH_URL}/v1/refresh"
NEW_PAIR="$(auth_refresh "$REFRESH")"
NEW_ACCESS="$(echo "$NEW_PAIR" | jqx -r .access_token)"
[ -n "$NEW_ACCESS" ] && [ "$NEW_ACCESS" != "null" ] || fail "refresh did not return access_token"
[ "$(capi_code "$NEW_ACCESS" "$HZ")" = "200" ] \
  && ok "refreshed access works without client_id/secret again" || fail "refreshed token must work"

REUSE_BODY="$(printf '{"refresh_token":"%s"}' "$REFRESH")"
[ "$(auth_code POST /v1/refresh "$REUSE_BODY")" = "401" ] \
  && ok "rotated refresh token cannot be reused (401)" \
  || fail "old refresh token must be rejected after rotate"

step "G. Auth service rejects wrong client secret (401)"
show "POST /v1/token wrong secret"
WRONG_BODY="$(printf '{"client_id":"%s","client_secret":"wrong"}' "$AUTH_MEDIA_CLIENT_ID")"
[ "$(auth_code POST /v1/token "$WRONG_BODY")" = "401" ] \
  && ok "wrong client_secret -> 401" || fail "invalid client credentials must be 401"

ok "P3 client auth and field policy demo complete"
