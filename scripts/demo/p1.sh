#!/usr/bin/env bash
# Problem 1 - schema evolution demo (live, no restart, self-verifying).
#
# Sequence required by the spec:
#   system up -> gateway works with the OLD PVMBG schema -> schema change is
#   triggered -> gateway keeps working WITHOUT restart -> unknown-field policy holds.
#
# The mapping is verified against an INDEPENDENT oracle: the script reads the raw
# records straight from BMKG/PVMBG, computes the expected HazardEvent itself (jq)
# and compares it with what the Aggregator stored.
#
# Prerequisite: `make up` (or docker compose up -d --build). For a pristine run use `make reset`.
. "$(dirname "$0")/../lib.sh"
require_stack

hz_url()   { echo "/internal/hazards?hazard_type=$1&limit=500"; }
count_of() { agg "$(hz_url "$1")" | jqx '.items|length'; }
quarantine_total() { agg /internal/quarantine | jqx '.total.PVMBG // 0'; }
has_hazard() { [ "$(code -H "X-Service-Key: ${AGGREGATOR_SERVICE_KEY}" "${AGG_URL}/internal/hazards/$1")" = "200" ]; }
seed_ready() { [ "$(count_of SEISMIC)" -ge 20 ] && [ "$(count_of VOLCANIC)" -ge 20 ]; }
started_all() { local s; for s in bmkg-mock pvmbg-mock aggregator; do echo "$s=$(started_at "$s")"; done; }

# ---------------------------------------------------------------- oracles ---
oracle_bmkg() {
  local ev wa hz
  ev="$(bmkg /seismic-events)"; wa="$(bmkg /tsunami-warnings)"; hz="$(agg "$(hz_url SEISMIC)")"
  printf '{"ev":%s,"wa":%s,"hz":%s}' "$ev" "$wa" "$hz" | jqx -c '
    def rank: {"WASPADA":1,"SIAGA":2,"AWAS":3}[.];
    (.hz.items | map({key:.source_ref_id, value:.}) | from_entries) as $H
    | .wa as $wa
    | [ .ev[] | . as $e | select($H[$e.event_id] != null)
        | $H[$e.event_id] as $h
        | ([$wa[] | select(.related_event_id == $e.event_id) | (.threat_level|ascii_upcase)] | sort_by(rank) | last) as $w
        | (if $w then $w elif $e.magnitude < 5 then "NORMAL" elif $e.magnitude < 6.5 then "WASPADA" else "SIAGA" end) as $exp
        | { id: $e.event_id, expected: $exp, got: $h.severity,
            ok: ( $h.severity == $exp
              and $h.hazard_id == ("bmkg-" + $e.event_id) and $h.source == "BMKG" and $h.hazard_type == "SEISMIC"
              and $h.source_ref_id == $e.event_id and $h.area_name == $e.region_name
              and $h.latitude == $e.epicenter_lat and $h.longitude == $e.epicenter_lon
              and ($h.occurred_at[0:19] == $e.occurred_at[0:19])
              and $h.attributes.magnitude == $e.magnitude and $h.attributes.depth_km == $e.depth_km
              and $h.attributes.potential_tsunami == $e.potential_tsunami
              and (($w == null) or ($h.attributes.warning_id != null)) ) } ] as $rows
    | { source_events: (.ev|length), checked: ($rows|length),
        by_severity: ($rows | group_by(.got) | map({(.[0].got): length}) | add),
        bad: [$rows[] | select(.ok|not)] }
    | . + {pass: ((.bad|length) == 0 and .checked >= 20)}'
}

oracle_pvmbg() { # oracle_pvmbg [space-separated report ids that must match EXACTLY (created during this run)]
  local rp hz tbl ids="${1:-}"
  rp="$(pvmbg /volcanic-reports)"; hz="$(agg "$(hz_url VOLCANIC)")"; tbl="$(cat services/aggregator/config/volcanoes.json)"
  printf '{"rp":%s,"hz":%s,"tbl":%s}' "$rp" "$hz" "$tbl" | jqx -c --arg ids "$ids" '
    ($ids | split(" ") | map(select(length>0))) as $X
    | (.hz.items | map({key:.source_ref_id, value:.}) | from_entries) as $H
    | .tbl as $T
    | [ .rp[] | . as $r | select($H[$r.report_id] != null)
        | $H[$r.report_id] as $h
        | ($r | del(.report_id, .volcano_id, .alert_level, .reported_at)) as $expAttrs
        | { id: $r.report_id,
            ok: ( $h.severity == ($r.alert_level|ascii_upcase)
              and $h.hazard_id == ("pvmbg-" + $r.report_id) and $h.source == "PVMBG" and $h.hazard_type == "VOLCANIC"
              and $h.source_ref_id == $r.report_id
              and $h.area_name == $T[$r.volcano_id].name
              and $h.latitude == $T[$r.volcano_id].latitude and $h.longitude == $T[$r.volcano_id].longitude
              and ($h.occurred_at[0:19] == $r.reported_at[0:19])
              and ($expAttrs | to_entries | all(.value == $h.attributes[.key]))
              and (($X | index($r.report_id)) == null or ($h.attributes == $expAttrs)) ) } ] as $rows
    | { source_reports: (.rp|length), checked: ($rows|length),
        with_confidence_level: ([.hz.items[] | select(.attributes.confidence_level != null)] | length),
        bad: [$rows[] | select(.ok|not)] }
    | . + {pass: ((.bad|length) == 0 and .checked >= 20)}'
}

# verify NAME ORACLE_FN - ingestion races with the check, so retry a few times
verify() { # verify NAME ORACLE_FN [ORACLE_ARGS...]
  local name="$1" fn="$2" out="" i
  shift 2
  for i in 1 2 3 4 5 6; do
    out="$($fn "$@")"
    if echo "$out" | jqx -e '.pass' >/dev/null; then
      ok "$name: $(echo "$out" | jqx -c 'del(.bad)')"
      return 0
    fi
    sleep 3
  done
  echo "$out" | jqx . >&2
  fail "$name: mapping differs from the independent oracle"
}

# ---------------------------------------------------------------- scenario ---
step "Start from the ORIGINAL PVMBG schema (no confidence_level)"
show "POST ${PVMBG_URL}/admin/schema-version {confidence_level:false, extra_fields:{}}"
pvmbg_admin /admin/schema-version '{"confidence_level":false,"extra_fields":{}}' | jqx -c '{schema_version, confidence_level, extra_fields}'
pvmbg_admin /admin/outage '{"enabled":false}' >/dev/null
wait_until "seed ingested (>=20 seismic and >=20 volcanic hazards)" 90 seed_ready
info "hazards in the Canonical Store (via the Aggregator API): SEISMIC=$(count_of SEISMIC) VOLCANIC=$(count_of VOLCANIC)"

step "Aggregator output with the OLD schema follows the mapping rules (both sources)"
info "example BMKG hazard (tsunami event with warning):"
agg "$(hz_url SEISMIC)" | jqx -c '[.items[] | select(.attributes.warning_id != null)][0] | {hazard_id,source,hazard_type,severity,area_name,latitude,longitude,occurred_at,attributes}'
info "example PVMBG hazard (initial schema):"
agg "$(hz_url VOLCANIC)" | jqx -c '[.items[] | select(.attributes.confidence_level == null)][0] | {hazard_id,source,hazard_type,severity,area_name,latitude,longitude,occurred_at,attributes}'
verify "BMKG  SeismicEvent -> HazardEvent (severity rule, coordinates, attributes)" oracle_bmkg
verify "PVMBG VolcanicReport -> HazardEvent (reference table, severity, attributes)" oracle_pvmbg
BEFORE_CONF="$(agg "$(hz_url VOLCANIC)" | jqx '[.items[] | select(.attributes.confidence_level != null)] | length')"
if [ "$BEFORE_CONF" = "0" ]; then
  ok "no stored volcanic hazard has confidence_level yet"
else
  warn "$BEFORE_CONF hazards already carry confidence_level (an earlier run) - use 'make reset' for a pristine demo"
fi

step "Remember what is running (proof that nothing restarts) and the current data"
STARTED_BEFORE="$(started_all)"
echo "$STARTED_BEFORE" | sed 's/^/   /'
SNAPSHOT="$(agg "$(hz_url VOLCANIC)" | jqx -c '.items | map({key:.hazard_id, value:.}) | from_entries')"
QUAR0="$(quarantine_total)"

step "Trigger the schema change WHILE RUNNING: PVMBG starts sending confidence_level + a brand-new field"
show "POST ${PVMBG_URL}/admin/schema-version {confidence_level:true, extra_fields:{lava_dome_volume_m3:125000}}"
pvmbg_admin /admin/schema-version '{"confidence_level":true,"extra_fields":{"lava_dome_volume_m3":125000}}' | jqx -c '{schema_version, confidence_level, extra_fields}'
NEW_IDS="$(pvmbg_admin /admin/generate '{"count":3}' | jqx -r '.generated[].report_id')"
info "PVMBG issued new reports: $(echo $NEW_IDS)"
for id in $NEW_IDS; do wait_until "Aggregator stores pvmbg-$id" 60 has_hazard "pvmbg-$id"; done

step "Aggregator still produces VALID HazardEvents - no crash, no restart, no code change"
FIRST="$(echo $NEW_IDS | cut -d' ' -f1)"
info "new-schema hazard as stored:"
agg "/internal/hazards/pvmbg-$FIRST" | jqx -c '{hazard_id,severity,area_name,latitude,longitude,occurred_at,attributes}'
verify "PVMBG mapping after the change (new reports: attributes == every non-canonical source field, incl. confidence_level and lava_dome_volume_m3; old ones: nothing lost)" oracle_pvmbg "$(echo $NEW_IDS)"
if agg "/internal/hazards/pvmbg-$FIRST" | jqx -e '.attributes.confidence_level != null and .attributes.lava_dome_volume_m3 == 125000' >/dev/null; then
  ok "confidence_level and the unknown field lava_dome_volume_m3 were forwarded to attributes (declared policy)"
else
  fail "unknown fields were not forwarded to attributes"
fi
STARTED_AFTER="$(started_all)"
if [ "$STARTED_BEFORE" = "$STARTED_AFTER" ]; then
  ok "bmkg-mock, pvmbg-mock and aggregator were NOT restarted (StartedAt unchanged)"
else
  fail "a container was restarted: $STARTED_AFTER"
fi
if [ "$(agg /internal/sources/status | jqx -r '[.sources[] | select(.state != "up")] | length')" = "0" ]; then
  ok "both sources are 'up' in /internal/sources/status"
else
  fail "a source is not up"
fi

step "Old records (stored BEFORE the change) are untouched and coexist with the new ones"
AFTER="$(agg "$(hz_url VOLCANIC)" | jqx -c '.items | map({key:.hazard_id, value:.}) | from_entries')"
if printf '{"a":%s,"b":%s}' "$SNAPSHOT" "$AFTER" | jqx -e '
     . as $r | [ $r.a | keys[] | select($r.b[.] != null and $r.b[.].version == $r.a[.].version and $r.b[.].attributes == $r.a[.].attributes) ] | length == ($r.a | length)' >/dev/null; then
  ok "all $(echo "$SNAPSHOT" | jqx 'length') pre-change volcanic hazards are unchanged (same version, same attributes)"
else
  fail "a record stored before the change was modified"
fi
LIST="$(agg "$(hz_url VOLCANIC)")"
info "side by side in the store: $(echo "$LIST" | jqx '[.items[] | select(.attributes.confidence_level != null)] | length') hazards WITH confidence_level, $(echo "$LIST" | jqx '[.items[] | select(.attributes.confidence_level == null)] | length') WITHOUT - both readable"

step "Consciously UNSUPPORTED changes are quarantined, never guessed - and never break the batch"
show "POST ${PVMBG_URL}/admin/generate {override:{alert_level:\"Darurat\"}}   # new enum value"
BAD_ID="$(pvmbg_admin /admin/generate '{"count":1,"override":{"alert_level":"Darurat"}}' | jqx -r '.generated[0].report_id')"
GOOD_ID="$(pvmbg_admin /admin/generate '{"count":1}' | jqx -r '.generated[0].report_id')"
wait_until "valid report pvmbg-$GOOD_ID stored" 60 has_hazard "pvmbg-$GOOD_ID"
ok "the valid report issued right after the bad one was stored (the batch was not blocked)"
if has_hazard "pvmbg-$BAD_ID"; then fail "unsupported enum value was stored"; else ok "report $BAD_ID with alert_level=Darurat was NOT stored"; fi
agg /internal/quarantine | jqx -c '.recent[0] | {source,record_id,field,reason,correlation_id}'
if [ "$(quarantine_total)" -gt "$QUAR0" ]; then
  ok "quarantine counter increased ($QUAR0 -> $(quarantine_total))"
else
  fail "quarantine counter did not increase"
fi

step "One traceable flow: the same correlation ID from the PVMBG call to the stored hazard to the broker outbox"
CID="$(docker compose logs --no-log-prefix aggregator 2>/dev/null | grep '"hazard_stored"' | grep "pvmbg-$GOOD_ID" | tail -1 | jqx -r '.cid')"
info "correlation id: $CID"
FLOW="$(docker compose logs --no-log-prefix aggregator 2>/dev/null | grep "\"cid\":\"$CID\"")"
echo "$FLOW" | jqx -c '{ts,msg,target,latency_ms,status,hazard_id,version,outcome,event_id,fetched,created,quarantined} | with_entries(select(.value != null))' | sed 's/^/   /'
for m in outbound_call hazard_stored poll_ok; do
  echo "$FLOW" | grep -q "\"msg\":\"$m\"" || fail "log line '$m' missing for cid $CID"
done
ok "poll -> outbound call (latency logged) -> hazard_stored -> poll_ok share correlation ID $CID"

step "Restore the original schema on PVMBG (leaves the system reusable)"
pvmbg_admin /admin/schema-version '{"confidence_level":false,"extra_fields":{}}' | jqx -c '{schema_version, extra_fields}'
printf '\n%sP1 PASS%s - schema evolution handled live, per the declared policy (additive-only; unknown fields -> attributes; breaking changes -> quarantine).\n' "$C_G$C_B" "$C_0"
