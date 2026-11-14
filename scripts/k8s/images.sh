#!/usr/bin/env bash
# Build every service image from this checkout and make it available on every node.
#   bash scripts/k8s/images.sh [service ...]      (default: all services)
# See scripts/k8s/common.sh for IMAGE_TAG / IMAGE_REGISTRY / K8S_IMAGE_MODE / K8S_SSH_NODES.
. "$(dirname "${BASH_SOURCE[0]}")/common.sh"

targets=("$@")
[ ${#targets[@]} -eq 0 ] && targets=("${SERVICES[@]}")

for svc in "${targets[@]}"; do
  img="${IMAGE_PREFIX}argus/$svc:$IMAGE_TAG"
  say "build $img"
  docker build -q -f "$ROOT/services/$svc/Dockerfile" --build-arg "VERSION=$IMAGE_TAG" -t "$img" "$ROOT" >/dev/null
  case "$K8S_IMAGE_MODE" in
    registry)
      docker push -q "$img" >/dev/null
      ;;
    kind)
      cluster="${KUBE_CONTEXT#kind-}"
      for node in $(docker ps --filter "label=io.x-k8s.kind.cluster=$cluster" --format '{{.Names}}'); do
        docker save "$img" | docker exec -i "$node" ctr -n k8s.io images import --digests - >/dev/null
      done
      ;;
    ssh)
      [ -n "${K8S_SSH_NODES:-}" ] || die "K8S_SSH_NODES is empty (e.g. \"ubuntu@node-a ubuntu@node-b\") or set IMAGE_REGISTRY"
      for node in $K8S_SSH_NODES; do
        # K8S_SSH_OPTS: e.g. "-F ~/.ssh/argus-k3s.config" (written by scripts/k8s/vbox-cluster.sh)
        # shellcheck disable=SC2086
        docker save "$img" | ssh ${K8S_SSH_OPTS:-} "$node" 'sudo k3s ctr images import -' >/dev/null
      done
      ;;
    *) die "unknown K8S_IMAGE_MODE=$K8S_IMAGE_MODE" ;;
  esac
done
say "images ready: tag $IMAGE_TAG ($K8S_IMAGE_MODE)"
