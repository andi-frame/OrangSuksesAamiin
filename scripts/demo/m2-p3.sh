#!/usr/bin/env bash
# M2 Problem 3 (ingest side) on the running cluster (make k8s-up first). Self-checking.
#   1. declared policy: a compatible change is accepted automatically
#   2. broken messages (deep, invalid UTF-8, oversize, cut off) crash nothing; the others keep flowing;
#      the cut-off one is retryable and recovers by itself, the others are parked
#   3. failed messages can be inspected only by BNPB Internal Ops (raw bytes), the auditor sees metadata
#   4. an incompatible schema change (rename, type change, required field missing) is parked, not guessed
#   5. the system is adjusted (schema rules, rolling update), Ops requeues: all recover, no duplicate
. "$(dirname "${BASH_SOURCE[0]}")/k8s-lib.sh"

kn rollout status deploy/aggregator --timeout=180s >/dev/null
forward aggregator 18083 8083
forward pvmbg-mock 18082 8082
pvmbg_admin /admin/schema-version '{"version":1}' >/dev/null

open_count() { ops "/internal/ops/unprocessed?source=PVMBG&status=$1&limit=1000&raw=false" | jqx '.items | length'; }
hazard() { agg "/internal/hazards/pvmbg-$1"; }
restarts_now=$(restarts aggregator)

step "Declared policy (services/aggregator/internal/mapping/raw.go)"
info "accepted automatically: new fields of any type -> attributes; attribute fields changing type or disappearing"
info "not accepted (parked, never guessed): rename / removal / move / type change of a canonical field, new enum values"
info "unreadable bytes (cut off, invalid UTF-8, too deep, too large) -> parked as malformed; cut off = retryable"
show "GET /internal/ops/schema-rules"
ops /internal/ops/schema-rules | sed 's/^/   /'
show 'POST pvmbg-mock /admin/schema-version {"confidence_level":true, "extra_fields":{"lava_dome_volume_m3":125000}}'
pvmbg_admin /admin/schema-version '{"confidence_level":true,"extra_fields":{"lava_dome_volume_m3":125000}}' >/dev/null
id=$(pvmbg_admin /admin/generate '{"count":1}' | jqx -r '.generated[0].report_id')
wait_until 30 "report $id stored" agg_has "/internal/hazards/pvmbg-$id"
hazard "$id" | jqx -c '{hazard_id, severity, area_name, attributes}' | sed 's/^/   /'
[ "$(hazard "$id" | jqx '.attributes.lava_dome_volume_m3')" = 125000 ] || fail "new field not kept"
ok "compatible change accepted automatically (new fields in attributes)"
pvmbg_admin /admin/schema-version '{"version":1,"extra_fields":{}}' >/dev/null

step "Broken messages: nothing crashes, the rest keeps flowing"
for body in '{"kind":"deep","depth":200000}' '{"kind":"invalid_utf8"}' '{"kind":"oversize","size_bytes":3145728}' '{"kind":"truncated"}'; do
  show "POST pvmbg-mock /admin/malformed $body"
  pvmbg_admin /admin/malformed "$body" | jqx -c . | sed 's/^/   /'
done
good=$(pvmbg_admin /admin/generate '{"count":1}' | jqx -r '.generated[0].report_id')
wait_until 60 "valid report $good stored after the broken ones" agg_has "/internal/hazards/pvmbg-$good"
ok "a valid report sent together with the broken ones was stored ($good)"
sleep 5
show "GET /internal/ops/unprocessed?source=PVMBG&status=all (Internal Ops)"
ops "/internal/ops/unprocessed?source=PVMBG&status=all&raw=false" |
  jqx -r '.items[] | "   \(.status)\tretryable=\(.retryable)\tattempts=\(.attempts)\t\(.raw_size)B\t\(.reason[0:90])"'
for r in too_deep invalid_utf8 too_large; do
  ops "/internal/ops/unprocessed?source=PVMBG&status=parked&raw=false&limit=1000" | jqx -e --arg r "$r" \
    '[.items[] | select(.reason | contains($r))] | length > 0' >/dev/null || fail "no parked message for $r"
done
fragment_resolved() { ops "/internal/ops/unprocessed?source=PVMBG&status=resolved&raw=false&limit=1000" | grep -q 'truncated in transit'; }
wait_until 60 "cut-off message resolved by itself" fragment_resolved
ok "cut-off message: retrying -> resolved once PVMBG delivered it whole (no intervention)"
[ "$(restarts aggregator)" = "$restarts_now" ] || fail "an Aggregator restarted"
kn get pods -l app=aggregator -o custom-columns=POD:.metadata.name,RESTARTS:.status.containerStatuses[0].restartCount | sed 's/^/   /'
ok "no Aggregator crashed or restarted"
parked_before=$(ops "/internal/ops/unprocessed?source=PVMBG&status=parked&raw=false" | jqx '[.items[].attempts] | add')
sleep 8
parked_after=$(ops "/internal/ops/unprocessed?source=PVMBG&status=parked&raw=false" | jqx '[.items[].attempts] | add')
[ "$parked_before" = "$parked_after" ] || fail "parked messages were retried automatically"
ok "parked messages are not retried automatically (attempts unchanged); retryable ones are"

step "Who may look at failed messages"
check_access() { # <who> <expected: allow|deny> <curl header>
  local got
  got=$(code -H "$3" "$AGG/internal/ops/unprocessed")
  info "/internal/ops/unprocessed as $1: HTTP $got"
  if [ "$2" = allow ]; then [ "$got" = 200 ] || fail "$1 denied"; else [ "$got" != 200 ] || fail "$1 may read failed messages"; fi
}
check_access "Client-Facing API (X-Service-Key)" deny "X-Service-Key: $AGGREGATOR_SERVICE_KEY"
check_access "Client-Facing API key as Bearer" deny "Authorization: Bearer $AGGREGATOR_SERVICE_KEY"
check_access "auditor token" deny "Authorization: Bearer $AUDIT_TOKEN"
check_access "BNPB Internal Ops token" allow "Authorization: Bearer $AGGREGATOR_OPS_TOKEN"
show "GET /internal/audit/unprocessed (auditor: metadata only, Lampiran A)"
audit "/internal/audit/unprocessed?source=PVMBG&limit=2" | jqx -c . | cut -c1-400 | sed 's/^/   /'
ok "only Internal Ops sees the messages and their bytes"

step "Incompatible change: rename, type change, required field missing"
show 'POST pvmbg-mock /admin/schema-version {"mode":"incompatible"}'
pvmbg_admin /admin/schema-version '{"mode":"incompatible"}' | jqx -c '{schema_version, incompatible_changes}' | sed 's/^/   /'
ids=$(pvmbg_admin /admin/generate '{"count":3}' | jqx -r '.generated[].report_id')
echo "$ids" | sed 's/^/   generated /'
pvmbg_admin /admin/schema-version '{"version":1}' >/dev/null
parked_key() { ops "/internal/ops/unprocessed?source=PVMBG&status=parked&raw=false&limit=1000" | grep -q "\"record_key\":\"$1\""; }
for i in $ids; do
  wait_until 60 "$i parked" parked_key "$i"
  [ "$(code -H "X-Service-Key: $AGGREGATOR_SERVICE_KEY" "$AGG/internal/hazards/pvmbg-$i")" = 404 ] || fail "$i was stored by guessing"
done
first=$(echo "$ids" | head -1)
ops "/internal/ops/unprocessed?source=PVMBG&status=parked&limit=1000" | jqx -r --arg id "$first" \
  '.items[] | select(.record_key == $id) | "   reason: \(.reason)\n   raw:    \(.raw_text)"'
ok "3 incompatible reports parked with their exact bytes, none guessed"

step "Adjust the system: declare the new schema (deploy/schema-rules/pvmbg-v3.json), roll out, requeue"
cat "$ROOT/deploy/schema-rules/pvmbg-v3.json" | sed 's/^/   /'
kn create configmap aggregator-schema-rules-pvmbg-v3 --from-file=schema-rules.json="$ROOT/deploy/schema-rules/pvmbg-v3.json" \
  --dry-run=client -o yaml | kn apply -f - >/dev/null
show "kubectl -n $NS patch deploy/aggregator: mount aggregator-schema-rules-pvmbg-v3 (rolling update)"
kn patch deploy aggregator --type=json \
  -p '[{"op":"replace","path":"/spec/template/spec/volumes/0/configMap/name","value":"aggregator-schema-rules-pvmbg-v3"}]' >/dev/null
kn rollout status deploy/aggregator --timeout=180s
wait_until 90 "old pods gone and the lease on a new pod" settled
info "leader $(leader) loaded:"
kn exec "$(leader)" -- wget -qO- --header "Authorization: Bearer $AGGREGATOR_OPS_TOKEN" http://127.0.0.1:8083/internal/ops/schema-rules | sed 's/^/   /'
show 'POST /internal/ops/unprocessed/reprocess {"source":"PVMBG"}'
ops_post /internal/ops/unprocessed/reprocess '{"source":"PVMBG"}' | jqx -c . | sed 's/^/   /'
for i in $ids; do
  wait_until 60 "$i recovered" agg_has "/internal/hazards/pvmbg-$i"
  hazard "$i" | jqx -c '{hazard_id, severity, area_name, occurred_at, version}' | sed 's/^/   /'
done
dups=$(psql_q "SELECT count(*) - count(DISTINCT source_ref_id) FROM hazards WHERE source='PVMBG'")
[ "$dups" = 0 ] || fail "duplicates after reprocessing"
ok "all incompatible reports recovered after the adjustment, no duplicate (hazard_id is deterministic)"
show "GET /internal/ops/unprocessed?source=PVMBG (still open)"
ops "/internal/ops/unprocessed?source=PVMBG&raw=false" | jqx -r '.items[] | "   \(.status)\t\(.reason[0:80])"'
info "still parked: only the malformed messages (their bytes cannot be read by any schema rule)"
psql_table "SELECT status, retryable, count(*) FROM unprocessed WHERE source='PVMBG' GROUP BY 1,2 ORDER BY 1"

printf '\n%sM2 P3 PASS%s\n' "$C_G$C_B" "$C_0"
