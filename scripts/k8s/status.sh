#!/usr/bin/env bash
# Where everything runs, who leads, and what the leader thinks of the upstreams.
. "$(dirname "${BASH_SOURCE[0]}")/common.sh"
load_env
say "nodes"
kc get nodes -o wide
say "pods per node (role label)"
kn get pods -o wide -L role --sort-by=.spec.nodeName
say "leader (Lease aggregator-ingest)"
kn get lease aggregator-ingest -o custom-columns=HOLDER:.spec.holderIdentity,TERM:.spec.leaseTransitions,RENEWED:.spec.renewTime 2>/dev/null ||
  echo "   (no lease yet)"
say "aggregator endpoints behind the Service"
kn get endpointslices -l kubernetes.io/service-name=aggregator -o jsonpath='{range .items[*].endpoints[*]}{.targetRef.name} ready={.conditions.ready}{"\n"}{end}'
