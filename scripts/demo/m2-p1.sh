#!/usr/bin/env bash
# Snapshot the live store before, during, or after a practice scenario.
. "$(dirname "${BASH_SOURCE[0]}")/k8s-lib.sh"
[ "$K8S_STORAGE" = sharded ] || fail "select K8S_STORAGE=sharded"
evidence="$ROOT/docs/evidence/m2/p1-${1:-snapshot}.log"
mkdir -p "$(dirname "$evidence")"
exec > >(tee "$evidence") 2>&1
step "Canonical store replica placement and load"
kn get pods -l app=canonical-shard -o wide -L shard,db-role
pods=$(kn get pods -l app=canonical-shard -o jsonpath='{.items[*].metadata.name}')
for pod in $pods; do
  info "$pod"
  store_sql "$pod" -c "SELECT current_setting('argus.node') AS node,
    pg_is_in_recovery() AS replica,
    (SELECT count(*) FROM hazards WHERE NOT retired) AS active_rows,
    pg_total_relation_size('hazards') AS bytes,
    (SELECT n_tup_ins + n_tup_upd FROM pg_stat_user_tables WHERE relname='hazards') AS logical_writes,
    CASE WHEN pg_is_in_recovery() THEN pg_last_wal_replay_lsn() ELSE pg_current_wal_lsn() END AS lsn" ||
    info "unit not reachable"
done

forward aggregator 18083 8083
step "Read newest 50 and verify keyset order"
agg '/internal/hazards?limit=50' | jqx -e '
  {partial, unavailable_units, count:(.items|length),
   ordered:([.items[] | [(.occurred_at | sub("Z$"; "") | split(".") |
     .[0] + "." + (((.[1] // "") + "000000000")[0:9])), .hazard_id]] |
     . == (sort | reverse)), next_cursor} | select(.ordered)'

step "Audit checksum and locations"
audit '/internal/audit/checksum' | jqx -e 'select(.checksum | test("^[0-9a-f]{64}$"))'
if [ -n "${AUDIT_HAZARD_ID:-}" ]; then
  audit "/internal/audit/location?hazard_id=$AUDIT_HAZARD_ID" | jqx -e 'select(has("locations"))'
fi
