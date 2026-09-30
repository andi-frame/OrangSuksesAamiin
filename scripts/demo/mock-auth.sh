#!/usr/bin/env bash
# Credentials are per agency: a credential valid for BMKG is rejected by PVMBG (401/403)
# and vice versa; the internal service key opens neither; and each container only
# holds the secrets of its own trust domain. (Mocks are Orang 1's part; the
# downstream-client half of Problem 3 is Orang 2's.)
. "$(dirname "$0")/../lib.sh"
require_stack

expect() { # expect "description" EXPECTED_CODES actual
  case " $2 " in
    *" $3 "*) ok "$1 -> HTTP $3" ;;
    *) fail "$1 -> HTTP $3 (expected $2)" ;;
  esac
}
BAD_TOKEN="${PVMBG_GATEWAY_TOKEN%?}X" # valid structure, tampered signature

step "BMKG mock (X-BMKG-Key)"
expect "own key                        " "200"     "$(code -H "X-BMKG-Key: ${BMKG_API_KEY}" "${BMKG_URL}/seismic-events")"
expect "no credential                  " "401"     "$(code "${BMKG_URL}/seismic-events")"
expect "wrong key                      " "401 403" "$(code -H "X-BMKG-Key: not-the-key" "${BMKG_URL}/seismic-events")"
expect "PVMBG bearer token             " "401 403" "$(code -H "Authorization: Bearer ${PVMBG_GATEWAY_TOKEN}" "${BMKG_URL}/seismic-events")"
expect "PVMBG bearer token (warnings)  " "401 403" "$(code -H "Authorization: Bearer ${PVMBG_GATEWAY_TOKEN}" "${BMKG_URL}/tsunami-warnings")"
expect "aggregator service key         " "401 403" "$(code -H "X-Service-Key: ${AGGREGATOR_SERVICE_KEY}" "${BMKG_URL}/seismic-events")"
expect "PVMBG admin token as BMKG key  " "401 403" "$(code -H "X-BMKG-Key: ${PVMBG_ADMIN_TOKEN}" "${BMKG_URL}/seismic-events")"

step "PVMBG mock (Authorization: Bearer <pvmbg-token>)"
expect "own token                      " "200"     "$(code -H "Authorization: Bearer ${PVMBG_GATEWAY_TOKEN}" "${PVMBG_URL}/volcanic-reports")"
expect "no credential                  " "401"     "$(code "${PVMBG_URL}/volcanic-reports")"
expect "BMKG key in X-BMKG-Key         " "401 403" "$(code -H "X-BMKG-Key: ${BMKG_API_KEY}" "${PVMBG_URL}/volcanic-reports")"
expect "BMKG key presented as bearer   " "401 403" "$(code -H "Authorization: Bearer ${BMKG_API_KEY}" "${PVMBG_URL}/volcanic-reports")"
expect "tampered PVMBG token           " "401 403" "$(code -H "Authorization: Bearer ${BAD_TOKEN}" "${PVMBG_URL}/volcanic-reports")"
expect "aggregator service key         " "401 403" "$(code -H "Authorization: Bearer ${AGGREGATOR_SERVICE_KEY}" "${PVMBG_URL}/volcanic-reports")"
info "token structure: $(echo "${PVMBG_GATEWAY_TOKEN}" | cut -d. -f1,2) . <signature>   (BMKG's credential is an opaque API key)"
info "claims: $(echo "${PVMBG_GATEWAY_TOKEN}" | cut -d. -f3 | tr '_-' '/+' | base64 -d 2>/dev/null | jqx -c .)"

step "Operator endpoints need their own token; the upstream credentials do not open them"
expect "PVMBG /admin without token     " "401"     "$(code -X POST "${PVMBG_URL}/admin/outage")"
expect "PVMBG /admin with gateway token" "401 403" "$(code -X POST -H "Authorization: Bearer ${PVMBG_GATEWAY_TOKEN}" "${PVMBG_URL}/admin/outage")"
expect "PVMBG /admin with BMKG admin   " "401 403" "$(code -X POST -H "X-Admin-Token: ${BMKG_ADMIN_TOKEN}" "${PVMBG_URL}/admin/outage")"
expect "BMKG  /admin with PVMBG admin  " "401 403" "$(code -X POST -H "X-Admin-Token: ${PVMBG_ADMIN_TOKEN}" "${BMKG_URL}/admin/generate")"

step "Aggregator internal API (X-Service-Key): the upstream credentials do not open it either"
expect "no credential                  " "401"     "$(code "${AGG_URL}/internal/hazards")"
expect "BMKG key                       " "401 403" "$(code -H "X-Service-Key: ${BMKG_API_KEY}" "${AGG_URL}/internal/hazards")"
expect "PVMBG token                    " "401 403" "$(code -H "X-Service-Key: ${PVMBG_GATEWAY_TOKEN}" "${AGG_URL}/internal/hazards")"
expect "service key                    " "200"     "$(code -H "X-Service-Key: ${AGGREGATOR_SERVICE_KEY}" "${AGG_URL}/internal/hazards")"

step "Each container only holds the secrets of its own trust domain (docker compose exec ... env)"
envof() { docker compose exec -T "$1" env 2>/dev/null | cut -d= -f1 | grep -E '^(BMKG|PVMBG|AGGREGATOR|CANONICAL|KAFKA)' | sort | tr '\n' ' '; }
has() { docker compose exec -T "$1" env 2>/dev/null | cut -d= -f1 | grep -qx "$2"; }
info "bmkg-mock  : $(envof bmkg-mock)"
info "pvmbg-mock : $(envof pvmbg-mock)"
info "aggregator : $(envof aggregator)"
has bmkg-mock PVMBG_TOKEN_SECRET && fail "bmkg-mock knows PVMBG's signing secret"
has bmkg-mock PVMBG_GATEWAY_TOKEN && fail "bmkg-mock holds a PVMBG token"
has bmkg-mock AGGREGATOR_SERVICE_KEY && fail "bmkg-mock holds the internal service key"
has pvmbg-mock BMKG_API_KEY && fail "pvmbg-mock knows BMKG's key"
has pvmbg-mock AGGREGATOR_SERVICE_KEY && fail "pvmbg-mock holds the internal service key"
has aggregator PVMBG_TOKEN_SECRET && fail "the aggregator knows PVMBG's signing secret (it must only hold a token)"
has aggregator PVMBG_ADMIN_TOKEN && fail "the aggregator holds an admin token"
ok "bmkg-mock has no PVMBG/internal secret; pvmbg-mock has no BMKG/internal secret; the aggregator holds tokens but cannot mint or administer"
printf '\n%sMOCK AUTH PASS%s\n' "$C_G$C_B" "$C_0"
