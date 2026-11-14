#!/usr/bin/env bash
# Remove the whole system from the cluster, data included (the namespace holds every PVC and the Lease,
# so the store's fencing table and the Lease start over together). The Tester Agent is removed too unless
# KEEP_TESTER=1.
. "$(dirname "${BASH_SOURCE[0]}")/common.sh"
say "deleting namespace $NS (all data)"
kc delete namespace "$NS" --ignore-not-found --wait=true
if [ "${KEEP_TESTER:-0}" != 1 ]; then
  kc delete -f "$TESTER_MANIFEST" --ignore-not-found >/dev/null 2>&1 || true
fi
say "done"
