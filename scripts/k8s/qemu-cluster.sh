#!/usr/bin/env bash
# Three Linux guests with independent kernels. Docker supplies the QEMU tools.
set -euo pipefail
umask 077
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
WORK="${ARGUS_QEMU_DIR:-$ROOT/.argus-k3s-linux}"
IMAGE=argus/k3s-qemu:local
PREFIX=argus-k3s-linux
UBUNTU_BASE=https://cloud-images.ubuntu.com/minimal/releases/noble/release
UBUNTU_IMAGE=ubuntu-24.04-minimal-cloudimg-amd64.img
NODES=(node-a node-b node-c)
RAM=("${ARGUS_QEMU_SERVER_RAM:-2304}" "${ARGUS_QEMU_WORKER_RAM:-1792}" "${ARGUS_QEMU_WORKER_RAM:-1792}")
API_PORT=26443

say() { printf '==> %s\n' "$*"; }
die() { printf 'ERROR: %s\n' "$*" >&2; exit 1; }
container() { printf '%s-%s\n' "$PREFIX" "$1"; }
ip_of() { printf '10.77.1.%s\n' "$((11 + $1))"; }
port_of() { printf '%s\n' "$((3221 + $1))"; }
qemu_tool() {
  docker run --rm --user "$(id -u):$(id -g)" -v "$WORK:/vm" "$IMAGE" "$@"
}
nssh() { ssh -F "$WORK/ssh_config" "$@"; }

prepare() {
  [ "$(uname -m)" = x86_64 ] || die "the bundled image targets x86-64"
  [ -r /dev/kvm ] && [ -w /dev/kvm ] || die "read/write access to /dev/kvm is required"
  for tool in docker curl ssh scp ssh-keygen openssl python3 sha256sum; do
    command -v "$tool" >/dev/null || die "missing $tool"
  done
  docker info --format '{{.ServerVersion}}' >/dev/null
  mkdir -p "$WORK"
  chmod 700 "$WORK"
  if ! docker image inspect "$IMAGE" >/dev/null 2>&1; then
    say "installing QEMU and cloud-init tooling into a local Docker image"
    docker build -t "$IMAGE" "$ROOT/scripts/k8s/qemu"
  fi
  if [ ! -f "$WORK/base.qcow2" ]; then
    say "downloading and verifying Ubuntu 24.04 Minimal"
    curl --fail --location --retry 3 "$UBUNTU_BASE/SHA256SUMS" -o "$WORK/ubuntu-sha256"
    curl --fail --location --retry 3 "$UBUNTU_BASE/$UBUNTU_IMAGE" -o "$WORK/$UBUNTU_IMAGE"
    (cd "$WORK" && awk -v name="$UBUNTU_IMAGE" '$2==name || $2=="*" name {print}' ubuntu-sha256 | sha256sum -c -)
    mv "$WORK/$UBUNTU_IMAGE" "$WORK/base.qcow2"
  fi
  if [ ! -f "$WORK/id_ed25519" ]; then
    ssh-keygen -q -t ed25519 -N '' -C argus-k3s-linux -f "$WORK/id_ed25519"
  fi
  if [ ! -s "$WORK/join-token" ]; then
    openssl rand -hex 32 > "$WORK/join-token"
  fi
  for i in "${!NODES[@]}"; do
    cat <<CONFIG
Host ${NODES[$i]}
  HostName 127.0.0.1
  Port $(port_of "$i")
  User ubuntu
  IdentityFile "$WORK/id_ed25519"
  IdentitiesOnly yes
  StrictHostKeyChecking accept-new
  UserKnownHostsFile "$WORK/known_hosts"
  ConnectTimeout 4
  BatchMode yes
CONFIG
  done > "$WORK/ssh_config"
}

seed() {
  local i=$1 node=${NODES[$1]} dir="$WORK/${NODES[$1]}"
  mkdir -p "$dir"
  cat > "$dir/meta-data" <<DATA
instance-id: argus-linux-$node
local-hostname: $node
DATA
  cat > "$dir/network-config" <<NETWORK
version: 2
ethernets:
  uplink:
    match:
      macaddress: "52:54:00:77:10:0$i"
    set-name: uplink
    dhcp4: true
  cluster0:
    match:
      macaddress: "52:54:00:77:20:0$i"
    set-name: cluster0
    addresses: [$(ip_of "$i")/24]
NETWORK
  cat > "$dir/user-data" <<DATA
#cloud-config
hostname: $node
manage_etc_hosts: true
ssh_pwauth: false
users:
  - default
ssh_authorized_keys:
  - $(cat "$WORK/id_ed25519.pub")
packages:
  - iptables
runcmd:
  - [swapoff, -a]
  - [ln, -sfn, xtables-nft-multi, /usr/sbin/iptables]
DATA
  qemu_tool cloud-localds -N "/vm/$node/network-config" "/vm/$node/seed.iso" \
    "/vm/$node/user-data" "/vm/$node/meta-data"
}

start_node() {
  local i=$1 node=${NODES[$1]} name nat disk
  name="$(container "$node")"
  if docker container inspect "$name" >/dev/null 2>&1; then
    [ "$(docker inspect --format '{{.State.Running}}' "$name")" = true ] || docker start "$name" >/dev/null
    return
  fi
  disk="/vm/$node/disk.qcow2"
  if [ ! -s "$WORK/$node/disk.qcow2" ]; then
    seed "$i"
    qemu_tool qemu-img create -f qcow2 -F qcow2 -b /vm/base.qcow2 "$disk" 20G
  fi
  if [ ! -s "$WORK/$node/uefi.fd" ]; then
    qemu_tool cp /usr/share/OVMF/OVMF_VARS_4M.fd "/vm/$node/uefi.fd"
  fi
  nat="user,id=wan,hostfwd=tcp:127.0.0.1:$(port_of "$i")-:22"
  if [ "$i" = 0 ]; then
    for port in 30080 30083 30084; do
      nat+=",hostfwd=tcp:127.0.0.1:$port-:$port"
    done
    nat+=",hostfwd=tcp:127.0.0.1:$API_PORT-:6443"
  fi
  say "starting $node with ${RAM[$i]} MiB RAM"
  docker run -d --name "$name" --network host --device /dev/kvm --group-add "$(stat -c %g /dev/kvm)" \
    --user "$(id -u):$(id -g)" -v "$WORK:/vm" "$IMAGE" \
    qemu-system-x86_64 -name "$node" -machine q35,accel=kvm -cpu host -smp 2 -m "${RAM[$i]}" \
    -display none -monitor none -qmp "unix:/vm/$node/qmp.sock,server=on,wait=off" \
    -serial "file:/vm/$node/console.log" \
    -device virtio-balloon-pci,free-page-reporting=on \
    -drive if=pflash,format=raw,readonly=on,file=/usr/share/OVMF/OVMF_CODE_4M.fd \
    -drive "if=pflash,format=raw,file=/vm/$node/uefi.fd" \
    -drive "if=virtio,cache=none,format=qcow2,file=$disk" \
    -drive "if=virtio,format=raw,readonly=on,file=/vm/$node/seed.iso" \
    -netdev "$nat" -device "virtio-net-pci,netdev=wan,mac=52:54:00:77:10:0$i" \
    -netdev socket,id=lan,mcast=239.192.77.1:17761,localaddr=127.0.0.1 \
    -device "virtio-net-pci,netdev=lan,mac=52:54:00:77:20:0$i" >/dev/null
}

wait_ssh() {
  local node=$1 deadline=$((SECONDS+240))
  until nssh "$node" true 2>/dev/null; do
    [ "$(docker inspect --format '{{.State.Running}}' "$(container "$node")")" = true ] ||
      die "$node stopped; inspect docker logs $(container "$node")"
    [ "$SECONDS" -lt "$deadline" ] || die "$node SSH timed out; inspect $WORK/$node/console.log"
    sleep 3
  done
  timeout 180 ssh -F "$WORK/ssh_config" "$node" 'cloud-init status --wait'
}

prepare_k3s() {
  if [ ! -s "$WORK/k3s-version" ]; then
    local version
    version=$(curl -fsS -o /dev/null -w '%{redirect_url}' https://update.k3s.io/v1-release/channels/stable)
    version="${version##*/}"
    version="${version//%2B/+}"
    [[ "$version" =~ ^v[0-9]+\.[0-9]+\.[0-9]+\+k3s[0-9]+$ ]] || die "invalid K3s release: $version"
    printf '%s\n' "$version" > "$WORK/k3s-version"
  fi
  local version
  version=$(cat "$WORK/k3s-version")
  if [ ! -x "$WORK/k3s" ]; then
    local release="https://github.com/k3s-io/k3s/releases/download/$version"
    curl -fL --retry 3 "$release/sha256sum-amd64.txt" -o "$WORK/k3s-sha256"
    curl -fL --retry 3 "$release/k3s" -o "$WORK/k3s"
    (cd "$WORK" && awk '$2=="k3s" || $2=="*k3s" {print}' k3s-sha256 | sha256sum -c -)
    chmod 700 "$WORK/k3s"
  fi
  if [ ! -s "$WORK/install-k3s.sh" ]; then
    curl -fL --retry 3 https://get.k3s.io -o "$WORK/install-k3s.sh"
  fi
  local cli=${version%%+*}
  if [ ! -x "$WORK/bin/kubectl" ]; then
    mkdir -p "$WORK/bin"
    curl -fL --retry 3 "https://dl.k8s.io/release/$cli/bin/linux/amd64/kubectl" -o "$WORK/bin/kubectl"
    curl -fL --retry 3 "https://dl.k8s.io/release/$cli/bin/linux/amd64/kubectl.sha256" -o "$WORK/bin/kubectl.sha256"
    (cd "$WORK/bin" && printf '%s  kubectl\n' "$(cat kubectl.sha256)" | sha256sum -c -)
    chmod 700 "$WORK/bin/kubectl"
  fi
}

install_node() {
  local i=$1 node=${NODES[$1]} mode=agent address
  address=$(ip_of "$i")
  [ "$i" != 0 ] || mode=server
  if nssh "$node" "sudo systemctl is-active --quiet k3s$([ "$i" = 0 ] || printf '%s' -agent)"; then
    return
  fi
  say "installing k3s $mode on $node"
  scp -q -F "$WORK/ssh_config" "$WORK/k3s" "$WORK/install-k3s.sh" "$node:/tmp/"
  nssh "$node" 'sudo install -m 0755 /tmp/k3s /usr/local/bin/k3s; sudo mkdir -p /etc/rancher/k3s'
  {
    printf 'token: "%s"\nnode-name: %s\nnode-ip: %s\nflannel-iface: cluster0\n' "$(cat "$WORK/join-token")" "$node" "$address"
    if [ "$mode" = server ]; then
      printf 'disable: [traefik, servicelb]\ntls-san: [127.0.0.1]\n'
    else
      printf 'server: https://%s:6443\n' "$(ip_of 0)"
    fi
  } | nssh "$node" 'sudo install -m 0600 /dev/stdin /etc/rancher/k3s/config.yaml'
  nssh "$node" "sudo env INSTALL_K3S_SKIP_DOWNLOAD=true sh /tmp/install-k3s.sh $mode"
}

use_cluster() {
  export PATH="$WORK/bin:$PATH"
  export KUBECONFIG="$WORK/kubeconfig" KUBE_CONTEXT=argus-k3s-linux
  export K8S_STORAGE=sharded K8S_IMAGE_MODE=ssh
  export K8S_SSH_NODES="${NODES[*]}" K8S_SSH_OPTS="-F $WORK/ssh_config"
}

up() {
  prepare
  for i in "${!NODES[@]}"; do start_node "$i"; done
  prepare_k3s
  for i in "${!NODES[@]}"; do wait_ssh "${NODES[$i]}"; done
  install_node 0
  install_node 1
  install_node 2
  nssh node-a 'sudo cat /etc/rancher/k3s/k3s.yaml' |
    sed -e "s#https://127.0.0.1:6443#https://127.0.0.1:$API_PORT#" \
      -e 's/: default$/: argus-k3s-linux/' > "$WORK/kubeconfig.new"
  mv "$WORK/kubeconfig.new" "$WORK/kubeconfig"
  use_cluster
  kubectl wait --for=condition=Ready --timeout=180s node/node-a node/node-b node/node-c
  bash "$ROOT/scripts/k8s/topology.sh"
}

qmp() {
  python3 "$ROOT/scripts/k8s/qemu/qmp.py" "$WORK/$1/qmp.sock" "$2"
}

case "${1:-}" in
  up) up ;;
  deploy) use_cluster; bash "$ROOT/scripts/k8s/up.sh" ;;
  status)
    use_cluster
    kubectl get nodes -o wide
    if kubectl get namespace sistem-informasi-kebencanaan-bnpb --ignore-not-found -o name | grep -q .; then
      bash "$ROOT/scripts/k8s/status.sh"
    fi
    ;;
  kubectl) use_cluster; shift; kubectl "$@" ;;
  exec) use_cluster; shift; "$@" ;;
  pause|resume)
    [[ "${2:-}" = node-b || "${2:-}" = node-c ]] || die "select a worker node: node-b or node-c"
    case "$1" in pause) qmp "$2" stop ;; resume) qmp "$2" cont ;; esac
    ;;
  stop)
    for node in node-c node-b node-a; do
      running=$(docker inspect -f '{{.State.Running}}' "$(container "$node")" 2>/dev/null || true)
      if [ "$running" = true ]; then
        qmp "$node" system_powerdown
        timeout 150 docker wait "$(container "$node")" >/dev/null
      fi
    done
    ;;
  *) die "usage: $0 {up|deploy|status|kubectl ...|exec ...|pause node-b|resume node-b|stop}" ;;
esac
