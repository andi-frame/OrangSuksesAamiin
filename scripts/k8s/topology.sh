#!/usr/bin/env bash
# Print the node part of the topology declaration (README §M2.1) from the live cluster: name, IP, kernel,
# OS, CPU, memory and boot id (a different boot id = a different kernel, i.e. a separate VM or machine).
# The physical host of each node and its CPU/RAM cannot be seen from Kubernetes: add them by hand.
. "$(dirname "${BASH_SOURCE[0]}")/common.sh"
kc get nodes -o custom-columns='NODE:.metadata.name,CONTROL-PLANE:.metadata.labels.node-role\.kubernetes\.io/control-plane,IP:.status.addresses[0].address,KERNEL:.status.nodeInfo.kernelVersion,OS:.status.nodeInfo.osImage,CPU:.status.capacity.cpu,MEMORY:.status.capacity.memory,BOOT-ID:.status.nodeInfo.bootID'
echo
nodes=$(kc get nodes -o name | wc -l | tr -d ' ')
kernels=$(kc get nodes -o jsonpath='{range .items[*]}{.status.nodeInfo.bootID}{"\n"}{end}' | sort -u | wc -l | tr -d ' ')
echo "$kernels distinct kernels on $nodes nodes"
[ "$kernels" = "$nodes" ] || echo "WARNING: nodes share a kernel (kind/k3d/minikube): not valid for the demo"
