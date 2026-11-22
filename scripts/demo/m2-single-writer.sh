#!/usr/bin/env bash
# M2 P2 criteria 2, 4 and 5 - run it AFTER any disturbance (Tester Agent clock-skew / kill-leader /
# partition, a frozen VM, ...). It judges only from places with ONE clock each:
#   - the upstream mocks' own request logs: callers never overlap in time;
#   - the store: write periods of different leadership terms never overlap (stored_at = store clock),
#     and no HazardEvent exists twice;
#   - the Aggregator logs: fenced writes and lost leaderships are listed.
#   SINCE=15m (log window, default 15m)
. "$(dirname "${BASH_SOURCE[0]}")/k8s-lib.sh"
SINCE="${SINCE:-15m}"

step "Lease and fencing state"
kn get lease aggregator-ingest -o custom-columns=HOLDER:.spec.holderIdentity,TERM:.spec.leaseTransitions,ACQUIRED:.spec.acquireTime
psql_table "SELECT name AS lease, term AS fenced_term, holder, raised_at FROM fence"

step "Upstream request logs: one caller at a time (last $SINCE, upstream clock)"
for mock in bmkg-mock pvmbg-mock; do
  periods=$(upstream_callers "$mock" "$SINCE")
  info "$mock"
  echo "$periods" | awk '{printf "     %-32s %4s requests  %s .. %s\n", $4, $3, $5, $6}'
  [ "$(echo "$periods" | overlapping)" = 0 ] || fail "$mock: two instances polled in the same period"
done
ok "no two instances ever polled an upstream in the same period"

step "Store: write periods per leadership term (store clock)"
term_periods
[ "$(term_overlaps)" = 0 ] || fail "two terms stored results in the same period"
ok "no two instances ever stored results in the same period"
[ "$(duplicate_hazards)" = 0 ] || fail "duplicate HazardEvent"
ok "no duplicate HazardEvent"

step "Aggregator logs: leadership changes and refused writes (last $SINCE)"
for p in $(kn get pods -l app=aggregator -o jsonpath='{.items[*].metadata.name}'); do
  kn logs "$p" --since="$SINCE" |
    grep -E '"msg":"(leader_acquired|lease_taken_over|leader_lost|fence_raised|fence_rejected|write_fenced|dlq_worker_fenced|poll_skipped)"' |
    jqx -r '"   \(.ts) \(.instance) \(.msg) \(.term // "") \(.reason // .error // "")"' | tail -12
done
printf '\n%sSINGLE WRITER PASS%s\n' "$C_G$C_B" "$C_0"
