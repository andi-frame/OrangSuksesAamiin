#!/usr/bin/env bash
# One command for the whole system on Kubernetes (README §M2):
#   build + load images -> namespace -> per-component Secrets -> manifests -> Tester Agent -> wait until ready.
# Re-running it is safe (everything is applied idempotently); a new IMAGE_TAG rolls the services out.
. "$(dirname "${BASH_SOURCE[0]}")/common.sh"

say "context $KUBE_CONTEXT, image tag $IMAGE_TAG, image mode $K8S_IMAGE_MODE"
kc get nodes -o wide

storage=(statefulset/postgres-canonical)
legacy=$(kn get statefulset postgres-canonical --ignore-not-found -o jsonpath='{.spec.replicas}')
legacy=${legacy:-0}
active_storage=$(kn get configmap aggregator-config --ignore-not-found -o jsonpath='{.data.REPOSITORY_BACKEND}')
migrate=0
if [ "$active_storage" = sharded ] && [ "$K8S_STORAGE" != sharded ]; then
  die "this cluster already uses sharded storage. Set K8S_STORAGE=sharded"
fi
if [ "$K8S_STORAGE" = sharded ] && [ "$legacy" -gt 0 ] && [ "$active_storage" != sharded ]; then
  [ "${MIGRATE_CANONICAL:-0}" = 1 ] ||
    die "legacy Canonical Store is running. Select MIGRATE_CANONICAL=1 to preserve its rows during cutover"
  migrate=1
fi
if [ "$K8S_STORAGE" = sharded ]; then
  # Storage nodes: STORE_NODES="node-a node-b" (default: every node of the cluster, at least 2). Each shard has two
  # members on different nodes (pod anti-affinity), so every shard always has a copy off its primary's node. The
  # preferred primary of the shards rotates over the storage nodes, so the write load is on more than one node
  # (P1 criterion 1: data is split per shard, not per node; works on the 2-node demo cluster).
  read -r -a nodes <<<"${STORE_NODES:-$(kc get nodes -o jsonpath='{.items[*].metadata.name}')}"
  [ "${#nodes[@]}" -ge 2 ] || die "sharded storage needs at least two nodes (STORE_NODES)"
  slots=(a b c d e f)
  placement=()
  for i in "${!nodes[@]}"; do
    kc label node "${nodes[$i]}" "bnpb.io/store-slot=${slots[$i]}" --overwrite >/dev/null
  done
  for s in 0 1 2; do
    placement+=("--from-literal=shard-$s=${nodes[$((s % ${#nodes[@]}))]}")
  done
  say "storage nodes ${nodes[*]}; preferred primaries: ${placement[*]//--from-literal=/}"
  storage=(statefulset/shard-0 statefulset/shard-1 statefulset/shard-2)
fi

if [ "${SKIP_IMAGES:-0}" != 1 ]; then
  bash "$ROOT/scripts/k8s/images.sh"
fi

say "namespace + secrets"
kc apply -f "$ROOT/deploy/k8s/namespace.yaml" >/dev/null
bash "$ROOT/scripts/k8s/secrets.sh"

if [ "$migrate" = 1 ]; then
  bash "$ROOT/scripts/k8s/migrate-canonical.sh"
fi

if [ "$K8S_STORAGE" = sharded ]; then
  kn create configmap shard-placement "${placement[@]}" --dry-run=client -o yaml | kc apply -f - >/dev/null
fi

say "manifests ($K8S_MANIFESTS)"
# a Job's template is immutable: a finished kafka-init is replaced (it is idempotent) so new topics get created
kn delete job kafka-init --ignore-not-found >/dev/null
render | kc apply -f -

if [ "${SKIP_TESTER:-0}" != 1 ]; then
  say "Tester Agent (pinned manifest, image by digest, unmodified)"
  kc apply -f "$TESTER_MANIFEST"
fi

say "waiting for the rollout"
for kind_name in "${storage[@]}" statefulset/kafka deployment/bmkg-mock deployment/pvmbg-mock \
  deployment/aggregator deployment/auth-service deployment/client-api deployment/consumer-dashboard deployment/consumer-notifier \
  deployment/field-reports deployment/sensor-pantai deployment/prometheus deployment/grafana deployment/jaeger; do
  kn rollout status "$kind_name" --timeout=300s
done
kn wait --for=condition=complete job/kafka-init --timeout=300s || true
if [ "${SKIP_TESTER:-0}" != 1 ]; then
  kc -n tester-agent rollout status ds/tester-agent --timeout=180s
fi

if [ "$K8S_STORAGE" = sharded ] && [ "$legacy" -gt 0 ]; then
  kn scale statefulset/postgres-canonical --replicas=0
fi

say "ready"
kn get pods -o wide -L role
cat <<EOF

Entry points (any node IP):
  Client-Facing API  http://<node-ip>:30080
  Auth Service       http://<node-ip>:30084
  Aggregator         http://<node-ip>:30083   (/internal/*: X-Service-Key; /internal/ops/*: ops token; /internal/audit/*: audit token)
  Analytics (P1)     http://<node-ip>:30080/dashboard
  Grafana (metrics)  http://<node-ip>:30030   Jaeger (traces) http://<node-ip>:30686   Prometheus http://<node-ip>:30090
Leader:   kubectl -n $NS get lease aggregator-ingest -o jsonpath='{.spec.holderIdentity}'
EOF
