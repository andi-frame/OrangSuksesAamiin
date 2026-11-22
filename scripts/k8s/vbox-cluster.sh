#!/usr/bin/env bash
# Build a k3s demo cluster out of VirtualBox VMs on this machine (separate kernels, as the M2 spec requires).
#
#   bash scripts/k8s/vbox-cluster.sh up       create (or start) the VMs, install k3s, add kubectl context "argus-k3s"
#   bash scripts/k8s/vbox-cluster.sh status   VM state and nodes
#   bash scripts/k8s/vbox-cluster.sh stop     power the VMs off (data kept)
#   bash scripts/k8s/vbox-cluster.sh delete   remove the VMs and their disks (asks first)
#
# Nodes (VBOX_NODES, default "node-a node-b"): node-a = control plane + worker, the others = workers.
# They sit on a VirtualBox NAT Network (10.77.0.0/24, no admin rights needed); the host reaches them through
# port forwards on 127.0.0.1: API server :16443, SSH :2221.. (node-a, node-b, ...), NodePorts 30080/30083/30084/30030/30686/30090.
# Ubuntu 24.04 cloud image + cloud-init (seed ISO made with genisoimage in WSL).
#   VBOX_MEM (MiB, default 4608)  VBOX_CPUS (default 4); disk = the cloud image's 10 GB
#   resize: shut the VMs down cleanly, apply VBOX_MEM/VBOX_CPUS, start them again (disks and data are kept)
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

VB="${VBOXMANAGE:-$(command -v VBoxManage || echo "/c/Program Files/Oracle/VirtualBox/VBoxManage.exe")}"
read -r -a NODES <<<"${VBOX_NODES:-node-a node-b}"
MEM="${VBOX_MEM:-4608}" CPUS="${VBOX_CPUS:-4}"
NET=argus-k3s NET_PREFIX=10.77.0
WORK="${ARGUS_K3S_DIR:-$HOME/.argus-k3s}"
KEY="$HOME/.ssh/argus_k3s"
SSH_CONFIG="$HOME/.ssh/argus-k3s.config"
CONTEXT=argus-k3s
OVA_URL=https://cloud-images.ubuntu.com/noble/current/noble-server-cloudimg-amd64.ova

say() { printf '\033[1;36m==> %s\033[0m\n' "$*"; }
die() { printf '\033[1;31mERROR:\033[0m %s\n' "$*" >&2; exit 1; }
vb() { "$VB" "$@"; }
winpath() { if command -v cygpath >/dev/null; then cygpath -w "$1"; else echo "$1"; fi; }
wslpath_of() { # Windows path -> /mnt/c/... for genisoimage inside WSL (forward slashes survive the wsl.exe call)
  local m="$1"
  command -v cygpath >/dev/null && m=$(cygpath -m "$1")
  wsl -d "${WSL_DISTRO:-Ubuntu}" -- wslpath -a "$m" | tr -d '\r'
}
ip_of() { echo "$NET_PREFIX.$((11 + $1))"; }
ssh_port() { echo $((2221 + $1)); }
vm_exists() { vb list vms | grep -q "\"$1\""; }
vm_running() { vb list runningvms | grep -q "\"$1\""; }
nssh() { ssh -F "$SSH_CONFIG" "$@"; }

prepare() {
  mkdir -p "$WORK" "$HOME/.ssh"
  [ -f "$KEY" ] || ssh-keygen -q -t ed25519 -N '' -C argus-k3s -f "$KEY"
  [ -f "$WORK/token" ] || head -c 24 /dev/urandom | od -An -tx1 | tr -d ' \n' >"$WORK/token"
  {
    for i in "${!NODES[@]}"; do
      printf 'Host %s\n  HostName 127.0.0.1\n  Port %s\n  User ubuntu\n  IdentityFile %s\n' \
        "${NODES[$i]}" "$(ssh_port "$i")" "$KEY"
      printf '  StrictHostKeyChecking no\n  UserKnownHostsFile /dev/null\n  LogLevel ERROR\n  ConnectTimeout 5\n'
    done
  } >"$SSH_CONFIG"
  if ! vb natnetwork list | grep -q "Name: *$NET\$"; then
    say "NAT network $NET ($NET_PREFIX.0/24)"
    vb natnetwork add --netname "$NET" --network "$NET_PREFIX.0/24" --enable --dhcp off
  fi
  local rules
  rules=$(vb natnetwork list)
  add_fwd() { echo "$rules" | grep -q "$1" || vb natnetwork modify --netname "$NET" --port-forward-4 "$1:tcp:[127.0.0.1]:$2:[$3]:$4"; }
  add_fwd api 16443 "$(ip_of 0)" 6443
  # NodePorts: Client-Facing API, Aggregator, Auth, Grafana, Jaeger, Prometheus (browser and k6 on the host)
  for p in 30080 30083 30084 30030 30686 30090; do add_fwd "np$p" "$p" "$(ip_of 0)" "$p"; done
  for i in "${!NODES[@]}"; do add_fwd "ssh-${NODES[$i]}" "$(ssh_port "$i")" "$(ip_of "$i")" 22; done
  if [ ! -f "$WORK/ubuntu.ova" ]; then
    say "downloading the Ubuntu 24.04 cloud image (~600 MB)"
    curl -fL --retry 3 -o "$WORK/ubuntu.ova.part" "$OVA_URL"
    mv "$WORK/ubuntu.ova.part" "$WORK/ubuntu.ova"
  fi
}

seed_iso() { # <index> -> path of the cloud-init seed ISO of that node
  local i=$1 name=${NODES[$1]} dir="$WORK/seed-${NODES[$1]}" install
  local token pub
  token=$(cat "$WORK/token")
  pub=$(cat "$KEY.pub")
  mkdir -p "$dir"
  if [ "$i" = 0 ]; then
    install="curl -sfL https://get.k3s.io | K3S_TOKEN=$token INSTALL_K3S_EXEC='server --disable traefik --disable servicelb --node-name $name --node-ip $(ip_of 0) --tls-san 127.0.0.1 --write-kubeconfig-mode 644' sh -"
  else
    install="until curl -sk https://$(ip_of 0):6443/ping >/dev/null; do sleep 5; done; curl -sfL https://get.k3s.io | K3S_URL=https://$(ip_of 0):6443 K3S_TOKEN=$token INSTALL_K3S_EXEC='agent --node-name $name --node-ip $(ip_of "$i")' sh -"
  fi
  printf 'instance-id: %s-1\nlocal-hostname: %s\n' "$name" "$name" >"$dir/meta-data"
  cat >"$dir/network-config" <<EOF
version: 2
ethernets:
  nic:
    match: {name: "en*"}
    addresses: [$(ip_of "$i")/24]
    routes: [{to: default, via: $NET_PREFIX.1}]
    nameservers: {addresses: [$NET_PREFIX.1, 1.1.1.1, 8.8.8.8]}
EOF
  cat >"$dir/user-data" <<EOF
#cloud-config
hostname: $name
users:
  - name: ubuntu
    sudo: ALL=(ALL) NOPASSWD:ALL
    shell: /bin/bash
    ssh_authorized_keys: ["$pub"]
ssh_pwauth: false
package_update: false
growpart: {mode: auto, devices: ["/"]}
runcmd:
  - swapoff -a
  # the Tester Agent looks for iptables under its host mount (/host/usr/sbin); the stock absolute symlink
  # via /etc/alternatives does not resolve there, so point it straight at the binary
  - [sh, -c, "cd /usr/sbin && ln -sfn xtables-nft-multi iptables"]
  - [sh, -c, "$install"]
EOF
  wsl -d "${WSL_DISTRO:-Ubuntu}" -- sh -c "cd '$(wslpath_of "$dir")' && genisoimage -quiet -output seed.iso -volid cidata -joliet -rock user-data meta-data network-config"
  echo "$dir/seed.iso"
}

# create: import the cloud image as is. Its disk is 10 GB; resizing the imported VMDK with VBoxManage left it
# unbootable (tested), so it is not resized.
create() { # <index>
  local i=$1 name=${NODES[$1]}
  say "creating VM $name ($(ip_of "$i"), ${MEM} MiB, ${CPUS} vCPU, 10 GB)"
  vb import "$(winpath "$WORK/ubuntu.ova")" --vsys 0 --vmname "$name" --cpus "$CPUS" --memory "$MEM" \
    --basefolder "$(winpath "$WORK/vms")" >/dev/null
  # the cloud image's console is ttyS0: without a serial port the boot stalls; the port writes to a log file,
  # which is also where cloud-init's progress can be read
  vb modifyvm "$name" --nic1 natnetwork --nat-network1 "$NET" --nictype1 virtio --audio-enabled off \
    --uart1 0x3F8 4 --uart-mode1 file "$(winpath "$WORK/$name-console.log")"
}

# attach_seed: give the VM its cloud-init seed ISO (idempotent: a VM created by an interrupted run gets it too)
attach_seed() { # <index>
  local i=$1 name=${NODES[$1]} ctl
  vb showvminfo "$name" --machinereadable | grep -q 'seed.iso' && return 0
  ctl=$(vb showvminfo "$name" --machinereadable | grep -E '^storagecontrollername0=' | cut -d'"' -f2)
  vb storageattach "$name" --storagectl "$ctl" --port 1 --device 0 --type dvddrive --medium "$(winpath "$(seed_iso "$i")")"
}

up() {
  [ -x "$VB" ] || die "VBoxManage not found (install VirtualBox or set VBOXMANAGE)"
  prepare
  for i in "${!NODES[@]}"; do
    vm_exists "${NODES[$i]}" || create "$i"
    attach_seed "$i"
    vm_running "${NODES[$i]}" || { say "starting ${NODES[$i]}"; vb startvm "${NODES[$i]}" --type headless >/dev/null; }
  done
  say "waiting for k3s on ${NODES[0]} (first boot installs k3s: a few minutes)"
  local deadline=$((SECONDS + 900))
  until nssh "${NODES[0]}" 'test -f /etc/rancher/k3s/k3s.yaml' 2>/dev/null; do
    [ $SECONDS -ge $deadline ] && die "k3s did not come up on ${NODES[0]} (see: ssh -F $SSH_CONFIG ${NODES[0]} 'sudo cat /var/log/cloud-init-output.log')"
    sleep 10
  done
  say "kubectl context $CONTEXT"
  nssh "${NODES[0]}" 'cat /etc/rancher/k3s/k3s.yaml' |
    sed -e 's#https://127.0.0.1:6443#https://127.0.0.1:16443#' -e "s#: default\$#: $CONTEXT#" >"$WORK/kubeconfig"
  mkdir -p "$HOME/.kube"
  [ -f "$HOME/.kube/config" ] && cp "$HOME/.kube/config" "$HOME/.kube/config.before-argus-k3s"
  KUBECONFIG="$(winpath "$HOME/.kube/config");$(winpath "$WORK/kubeconfig")" kubectl config view --flatten >"$WORK/merged"
  mv "$WORK/merged" "$HOME/.kube/config"
  say "waiting for ${#NODES[@]} Ready nodes"
  deadline=$((SECONDS + 900))
  until [ "$(kubectl --context "$CONTEXT" get nodes --no-headers 2>/dev/null | grep -c ' Ready')" = "${#NODES[@]}" ]; do
    [ $SECONDS -ge $deadline ] && die "not every node became Ready"
    sleep 10
  done
  kubectl --context "$CONTEXT" get nodes -o wide
  cat <<EOF

Cluster ready. Deploy with:
  KUBE_CONTEXT=$CONTEXT K8S_IMAGE_MODE=ssh K8S_SSH_NODES="${NODES[*]}" K8S_SSH_OPTS="-F $SSH_CONFIG" bash scripts/k8s/up.sh
Freeze a worker (P2 criterion 5):
  FREEZE_NODE=${NODES[1]:-node-b} FREEZE_CMD='"$VB" controlvm ${NODES[1]:-node-b} pause' THAW_CMD='"$VB" controlvm ${NODES[1]:-node-b} resume' bash scripts/demo/m2-freeze.sh
EOF
}

case "${1:-}" in
  up) up ;;
  status)
    for n in "${NODES[@]}"; do printf '%-8s %s\n' "$n" "$(vb showvminfo "$n" --machinereadable 2>/dev/null | grep '^VMState=' | cut -d'"' -f2)"; done
    kubectl --context "$CONTEXT" get nodes -o wide 2>/dev/null || true
    ;;
  stop) for n in "${NODES[@]}"; do vm_running "$n" && vb controlvm "$n" acpipowerbutton; done ;;
  resize)
    # the whole stack with sharded storage requests ~6.5 GiB (Lampiran B) besides k3s itself: 2 x 3 GiB is too small
    # a VM in the "saved" state cannot be modified: resume it so that it can shut down cleanly
    for n in "${NODES[@]}"; do
      vb showvminfo "$n" --machinereadable | grep -q '^VMState="saved"' && { say "resuming saved $n"; vb startvm "$n" --type headless >/dev/null; sleep 20; }
    done
    # clean shutdown over SSH (the cloud image may ignore the ACPI button while k3s stops); ACPI as fallback
    for n in "${NODES[@]}"; do vm_running "$n" && { say "shutting down $n"; nssh "$n" sudo systemctl poweroff 2>/dev/null || vb controlvm "$n" acpipowerbutton; }; done
    for n in "${NODES[@]}"; do
      for _ in $(seq 1 60); do vm_running "$n" || break; sleep 3; done
      vm_running "$n" && die "$n did not shut down within 3 minutes"
      say "$n: ${MEM} MiB, ${CPUS} vCPU"
      vb modifyvm "$n" --memory "$MEM" --cpus "$CPUS" --nictype1 virtio   # virtio-net: far less softirq than e1000
    done
    for n in "${NODES[@]}"; do vb startvm "$n" --type headless >/dev/null; done
    prepare   # idempotent: also adds port forwards that are missing on an older NAT network
    say "started; wait for: kubectl --context $CONTEXT get nodes"
    ;;
  delete)
    # only an interactive "yes" deletes anything (never a missing or redirected stdin)
    [ -t 0 ] || die "delete needs an interactive terminal"
    printf 'Delete VMs %s and their disks? Type "yes": ' "${NODES[*]}"
    answer=""
    read -r answer || true
    [ "$answer" = yes ] || die "aborted"
    for n in "${NODES[@]}"; do
      vm_running "$n" && vb controlvm "$n" poweroff
      vm_exists "$n" && vb unregistervm "$n" --delete
    done
    ;;
  *) sed -n '2,13p' "$0"; exit 2 ;;
esac
