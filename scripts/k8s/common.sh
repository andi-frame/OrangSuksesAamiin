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
SERVICES=(aggregator bmkg-mock pvmbg-mock auth-service client-api consumer-hazard field-reports)
# sharded (default, P1): 3 Patroni shards x 2 members; single: one PostgreSQL (development only)
K8S_STORAGE="${K8S_STORAGE:-sharded}"
K8S_MANIFESTS="$ROOT/deploy"
if [ "$K8S_STORAGE" = sharded ]; then
  K8S_MANIFESTS="$ROOT/overlays/sharded"
  SERVICES+=(canonical-shard)
fi

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
export IMAGE_TAG KUBE_CONTEXT K8S_STORAGE

if [ -z "${K8S_IMAGE_MODE:-}" ]; then
  case "$KUBE_CONTEXT" in kind-*) K8S_IMAGE_MODE=kind ;; *) K8S_IMAGE_MODE=ssh ;; esac
fi
[ -n "${IMAGE_REGISTRY:-}" ] && K8S_IMAGE_MODE=registry

say() { printf '\033[1;36m==> %s\033[0m\n' "$*"; }
die() { printf '\033[1;31mERROR:\033[0m %s\n' "$*" >&2; exit 1; }

# load_env exports every variable of .env (credentials included) for the scripts only.
# ENV_FILE=<path> uses another file (e.g. one per cluster); default: .env of this checkout.
ENV_FILE="${ENV_FILE:-$ROOT/.env}"
load_env() {
  [ -f "$ENV_FILE" ] || die "$ENV_FILE missing: cp .env.example .env"
  local tmp
  tmp="$(mktemp)"
  # tr: a .env saved with Windows line endings must not put '\r' into credentials
  # (a temporary file, not ". <(...)": sourcing a pipe does nothing in the bash 3.2 of macOS)
  tr -d '\r' <"$ENV_FILE" >"$tmp"
  set -a
  # shellcheck disable=SC1090
  . "$tmp"
  set +a
  rm -f "$tmp"
}

# render prints the manifests with the image names/tags of this deployment.
render() {
  kc kustomize "$K8S_MANIFESTS" |
    sed -E "s#image: argus/([a-z-]+):dev#image: ${IMAGE_PREFIX}argus/\1:${IMAGE_TAG}#"
}
