#!/usr/bin/env bash
# One command for the whole system on Kubernetes (README §M2):
#   build + load images -> namespace -> per-component Secrets -> manifests -> Tester Agent -> wait until ready.
# Re-running it is safe (everything is applied idempotently); a new IMAGE_TAG rolls the services out.
. "$(dirname "${BASH_SOURCE[0]}")/common.sh"

say "context $KUBE_CONTEXT, image tag $IMAGE_TAG, image mode $K8S_IMAGE_MODE"
kc get nodes -o wide

if [ "${SKIP_IMAGES:-0}" != 1 ]; then
  bash "$ROOT/scripts/k8s/images.sh"
fi

say "namespace + secrets"
kc apply -f "$ROOT/deploy/k8s/namespace.yaml" >/dev/null
bash "$ROOT/scripts/k8s/secrets.sh"

say "manifests (kubectl apply -k deploy/)"
render | kc apply -f -

if [ "${SKIP_TESTER:-0}" != 1 ]; then
  say "Tester Agent (pinned manifest, image by digest, unmodified)"
  kc apply -f "$TESTER_MANIFEST"
fi

say "waiting for the rollout"
for kind_name in statefulset/postgres-canonical statefulset/kafka deployment/bmkg-mock deployment/pvmbg-mock \
  deployment/aggregator deployment/auth-service deployment/client-api deployment/consumer-dashboard deployment/consumer-notifier; do
  kn rollout status "$kind_name" --timeout=300s
done
kn wait --for=condition=complete job/kafka-init --timeout=300s || true
if [ "${SKIP_TESTER:-0}" != 1 ]; then
  kc -n tester-agent rollout status ds/tester-agent --timeout=180s
fi

say "ready"
kn get pods -o wide -L role
cat <<EOF

Entry points (any node IP):
  Client-Facing API  http://<node-ip>:30080
  Auth Service       http://<node-ip>:30084
  Aggregator         http://<node-ip>:30083   (/internal/*: X-Service-Key; /internal/ops/*: ops token; /internal/audit/*: audit token)
Leader:   kubectl -n $NS get lease aggregator-ingest -o jsonpath='{.spec.holderIdentity}'
EOF
