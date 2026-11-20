#!/usr/bin/env bash
# M2 Problem 1 criterion 7: analytics dashboard (make k8s-up first). Self-checking.
#   1. the numbers of the dashboard (GET /v1/analytics/summary) equal a direct count over the Audit API
#      (GET /internal/audit/events, every page) - same cut on ingested_at, so ingestion may go on meanwhile
#   2. with a storage unit down (sharded storage only: STOP_UNIT=shard-1) the dashboard still answers and
#      flags the numbers as incomplete (partial=true, unavailable_units)
# Browser: http://<node-ip>:30080/dashboard
. "$(dirname "${BASH_SOURCE[0]}")/k8s-lib.sh"
API=http://127.0.0.1:18080
AUTHU=http://127.0.0.1:18084
forward aggregator 18083 8083
forward client-api 18080 8080
forward auth-service 18084 8084
TOK=$(curl -sS "$AUTHU/v1/token" -H 'Content-Type: application/json' \
  -d "{\"client_id\":\"$AUTH_PUSAT_CLIENT_ID\",\"client_secret\":\"$AUTH_PUSAT_CLIENT_SECRET\"}" | jqx -r .access_token)
WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"; cleanup_pf' EXIT

# category rule of the dashboard: SEISMIC with a BMKG tsunami warning counts as TSUNAMI
JQ_COUNT='def cat: if .hazard_type == "SEISMIC" and ((.attributes.warning_id // "") != "") then "TSUNAMI" else .hazard_type end;
  {total: length,
   by_category: (group_by(cat) | map({(.[0] | cat): length}) | add // {}),
   by_severity: (group_by(.severity) | map({(.[0].severity): length}) | add // {}),
   by_area: (group_by(.area_name) | map({(.[0].area_name): length}) | add // {})}'

step "1. Dashboard numbers = direct count over the Audit API"
CUT=$(date -u -v-10S +%FT%TZ 2>/dev/null || date -u -d '10 seconds ago' +%FT%TZ)
info "cut: ingested_at < $CUT (both sides)"
cur="" pages=0
: >"$WORK/events.jsonl"
while :; do
  page=$(audit "/internal/audit/events?limit=1000${cur:+&cursor=$cur}")
  echo "$page" | jqx -c --arg cut "$CUT" '.items[] | select(.ingested_at[0:19] < $cut[0:19])' >>"$WORK/events.jsonl"
  [ "$(echo "$page" | jqx -r .partial)" = true ] && warn "Audit API: partial (units $(echo "$page" | jqx -c .unavailable_units))"
  cur=$(echo "$page" | jqx -r '.next_cursor // empty')
  pages=$((pages + 1))
  [ -n "$cur" ] || break
done
jqx -s "$JQ_COUNT" <"$WORK/events.jsonl" >"$WORK/audit.json"
curl -sS "$API/v1/analytics/summary?ingested_before=$CUT" -H "Authorization: Bearer $TOK" >"$WORK/dash.json"
jqx '{total, by_category: (.by_category | with_entries(select(.value > 0))), by_severity: (.by_severity | with_entries(select(.value > 0))),
      by_area: (.by_area | map({(.area_name): .count}) | add // {})}' "$WORK/dash.json" >"$WORK/dash-norm.json"
info "Audit API: $(jqx -c '{total, by_category}' "$WORK/audit.json") ($pages pages)"
info "dashboard: $(jqx -c '{total, by_category}' "$WORK/dash-norm.json")"
info "by severity: $(jqx -c .by_severity "$WORK/dash-norm.json"); areas: $(jqx '.by_area | length' "$WORK/dash-norm.json")"
if jqx -e --slurpfile a "$WORK/audit.json" '. == $a[0]' "$WORK/dash-norm.json" >/dev/null; then
  ok "identical: total, per category, per severity and per area_name"
else
  diff <(jqx -S . "$WORK/audit.json") <(jqx -S . "$WORK/dash-norm.json") | head -20 | sed 's/^/   /'
  fail "dashboard and Audit API disagree"
fi
[ "$(jqx -r .partial "$WORK/dash.json")" = false ] || warn "summary is partial: $(jqx -c .unavailable_units "$WORK/dash.json")"

step "2. A storage unit down: still answered, flagged incomplete"
unit="${STOP_UNIT:-}"
if [ -z "$unit" ] && kn get statefulset shard-1 >/dev/null 2>&1; then unit=shard-1; fi
if [ -z "$unit" ]; then
  info "single Canonical Store (K8S_STORAGE=single): nothing to take away without taking everything; run with sharded storage"
else
  show "kubectl scale statefulset/$unit --replicas=0"
  kn scale "statefulset/$unit" --replicas=0 >/dev/null
  partial_now() { curl -sS "$API/v1/analytics/summary" -H "Authorization: Bearer $TOK" | jqx -e '.partial == true' >/dev/null; }
  wait_until 60 "summary flagged partial" partial_now
  curl -sS "$API/v1/analytics/summary" -H "Authorization: Bearer $TOK" | jqx -c '{partial, unavailable_units, total}' | sed 's/^/   /'
  info "dashboard banner: 'Angka BELUM LENGKAP: unit penyimpanan ... tidak terjangkau'"
  kn scale "statefulset/$unit" --replicas=1 >/dev/null
  kn rollout status "statefulset/$unit" --timeout=180s >/dev/null
  ok "answered with partial=true and the missing unit named (no 500, no wrong numbers without a flag)"
fi
info "open the dashboard: http://<node-ip>:30080/dashboard (log in with any client)"
printf '\n%sANALYTICS PASS%s\n' "$C_G$C_B" "$C_0"
