#!/usr/bin/env bash
# Rebind one wiped local claim after the target worker has been replaced.
. "$(dirname "${BASH_SOURCE[0]}")/common.sh"

[ "$#" -ge 4 ] && [ "$#" -le 5 ] && [ "$1" = --confirmed-wipe ] &&
  { [ "$#" -eq 4 ] || [ "$5" = --promote ]; } ||
  die "usage: $0 --confirmed-wipe <shard-0|shard-1|shard-2> <wiped-member> <replacement-node> [--promote]"
shard=$2 member=$3 replacement=$4
[[ "$shard" =~ ^shard-[0-2]$ && "$member" =~ ^$shard-[01]$ ]] || die "invalid shard/member"
case "$member" in *-0) survivor="$shard-1";; *) survivor="$shard-0";; esac

# Verify the survivor really owns this shard before any claim removal.
[ "$(kn get pod "$survivor" -o jsonpath='{.metadata.labels.db-role}')" = primary ] ||
  die "$survivor is not the current primary"
[ "$(kn exec "$survivor" -c postgres -- sh -c 'PGPASSWORD=$POSTGRES_PASSWORD psql -XqAt -h 127.0.0.1 -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "SELECT NOT pg_is_in_recovery()"')" = t ] ||
  die "$survivor did not answer as primary"
[ "$(kc get node "$replacement" -o jsonpath='{.status.conditions[?(@.type=="Ready")].status}')" = True ] ||
  die "$replacement is not Ready"
[ "$(kn get pod "$survivor" -o jsonpath='{.spec.nodeName}')" != "$replacement" ] ||
  die "replacement has to be on a different disk/node"
slot=$(kc get node "$replacement" -o jsonpath='{.metadata.labels.bnpb\.io/store-slot}')
[ -n "$slot" ] || die "replacement node must carry a bnpb.io/store-slot label (label it, or rerun scripts/k8s/up.sh)"
[ "$(kc get nodes -l "bnpb.io/store-slot=$slot" -o jsonpath='{.items[*].metadata.name}')" = "$replacement" ] ||
  die "the replacement node must be the only node with this store slot"

# Drain only the failed ordinal. Kubernetes PVC protection needs the pod gone.
# Scaling a two-member StatefulSet removes ordinal 1, so use a short-lived
# admission hold: remove its controller with orphaning, then recreate it.
manifest=$(mktemp)
trap 'rm -f "$manifest"' EXIT
kn get statefulset "$shard" -o json | jq 'del(.status,.metadata.uid,.metadata.resourceVersion,.metadata.creationTimestamp,.metadata.generation,.metadata.managedFields)' > "$manifest"
kn delete statefulset "$shard" --cascade=orphan --timeout=60s
trap 'kn apply -f "$manifest" >/dev/null; rm -f "$manifest"' EXIT
kn delete pod "$member" --ignore-not-found --grace-period=0 --force --timeout=30s
kn delete pvc "data-$member" --ignore-not-found --timeout=90s
kn apply -f "$manifest"
trap 'rm -f "$manifest"' EXIT
kn rollout status "statefulset/$shard" --timeout=300s
[ "$(kn get pod "$member" -o jsonpath='{.spec.nodeName}')" = "$replacement" ] ||
  die "$member did not bind to node $replacement"
wait_sync() {
  local primary=$1 standby=$2 deadline=$((SECONDS+120))
  until [ "$(kn exec -i "$primary" -c postgres -- sh -c 'psql -XqAt -U postgres -v member="$1"' _ "$standby" <<'SQL'
SELECT count(*) FROM pg_stat_replication WHERE application_name=:'member' AND sync_state='sync';
SQL
)" = 1 ]; do
    [ "$SECONDS" -lt "$deadline" ] || die "$standby did not become synchronous"
    sleep 2
  done
}

wait_sync "$survivor" "$member"
if [ "${5:-}" = --promote ]; then
  kn exec "$survivor" -c postgres -- patronictl -c /tmp/patroni.yaml switchover "$shard" \
    --leader "$survivor" --candidate "$member" --force
  wait_sync "$member" "$survivor"
fi
kn get pods -l "shard=$shard" -L db-role -o wide
