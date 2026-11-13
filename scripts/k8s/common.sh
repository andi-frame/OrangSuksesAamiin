#!/usr/bin/env bash
# Shared settings of the Kubernetes scripts. Sourced, not run.
#
#   KUBE_CONTEXT     kubectl context of the demo cluster (default: the current one)
#   IMAGE_TAG        tag of the images built from this checkout (default: git sha, +"-dirty-<time>" with local changes)
#   IMAGE_REGISTRY   when set, images are pushed there (e.g. ghcr.io/you) and the cluster pulls them
#   K8S_IMAGE_MODE   how images reach the nodes without a registry: kind | ssh   (default: kind if the context is kind-*)
#   K8S_SSH_NODES    for mode ssh: "user@node-a user@node-b"; each node runs `sudo k3s ctr images import -`
#   K8S_SSH_OPTS     extra ssh options, e.g. "-F ~/.ssh/argus-k3s.config" (VirtualBox cluster: scripts/k8s/vbox-cluster.sh)
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
. "$ROOT/scripts/winenv.sh"

NS=sistem-informasi-kebencanaan-bnpb
TESTER_MANIFEST=https://raw.githubusercontent.com/susTuna/if4031-tester-agent/ebb55261711123a9a211739ffbf140a275caa6da/manifest.yaml
SERVICES=(aggregator bmkg-mock pvmbg-mock auth-service client-api consumer-hazard)

KUBE_CONTEXT="${KUBE_CONTEXT:-$(kubectl config current-context)}"
kc() { kubectl --context "$KUBE_CONTEXT" "$@"; }
kn() { kubectl --context "$KUBE_CONTEXT" -n "$NS" "$@"; }

if [ -z "${IMAGE_TAG:-}" ]; then
  IMAGE_TAG="$(git -C "$ROOT" rev-parse --short HEAD 2>/dev/null || echo dev)"
  if [ -n "$(git -C "$ROOT" status --porcelain -- services pkg 2>/dev/null)" ]; then
    IMAGE_TAG="$IMAGE_TAG-dirty-$(date +%s)"
  fi
fi
IMAGE_PREFIX="${IMAGE_REGISTRY:+${IMAGE_REGISTRY%/}/}"

if [ -z "${K8S_IMAGE_MODE:-}" ]; then
  case "$KUBE_CONTEXT" in kind-*) K8S_IMAGE_MODE=kind ;; *) K8S_IMAGE_MODE=ssh ;; esac
fi
[ -n "${IMAGE_REGISTRY:-}" ] && K8S_IMAGE_MODE=registry

say() { printf '\033[1;36m==> %s\033[0m\n' "$*"; }
die() { printf '\033[1;31mERROR:\033[0m %s\n' "$*" >&2; exit 1; }

# load_env exports every variable of .env (credentials included) for the scripts only.
load_env() {
  [ -f "$ROOT/.env" ] || die ".env missing: cp .env.example .env"
  set -a
  # tr: a .env saved with Windows line endings must not put '\r' into credentials
  # shellcheck disable=SC1090
  . <(tr -d '\r' <"$ROOT/.env")
  set +a
}

# render prints the manifests with the image names/tags of this deployment.
render() {
  kc kustomize "$ROOT/deploy" |
    sed -E "s#image: argus/([a-z-]+):dev#image: ${IMAGE_PREFIX}argus/\1:${IMAGE_TAG}#"
}
