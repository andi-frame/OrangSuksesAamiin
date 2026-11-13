#!/usr/bin/env bash
# M2 P2 criterion 5: the node of the leader freezes (VM suspended, laptop lid closed) for longer than the
# lease, another instance takes over, the frozen one wakes up still believing it leads - and stores nothing.
#
# The freeze itself happens outside the cluster. Either give the commands:
#   FREEZE_CMD='VBoxManage controlvm node-b pause'  THAW_CMD='VBoxManage controlvm node-b resume'
#   (multipass: 'multipass suspend node-b' / 'multipass start node-b'; Hyper-V: Suspend-VM / Resume-VM)
# or leave them empty and the script asks you to do it by hand. FREEZE_SEC (default 40) > lease (15 s).
# FREEZE_NODE = the Kubernetes node the commands freeze (default: any worker). Only a worker may be frozen:
# the script first moves leadership onto an Aggregator pod on that node (by deleting the leader until the
# lease lands there).
. "$(dirname "${BASH_SOURCE[0]}")/k8s-lib.sh"
FREEZE_SEC="${FREEZE_SEC:-40}"

control=$(kc get nodes -l node-role.kubernetes.io/control-plane -o jsonpath='{.items[*].metadata.name}')
on_target() { # <node>
  if [ -n "${FREEZE_NODE:-}" ]; then [ "$1" = "$FREEZE_NODE" ]; else case " $control " in *" $1 "*) return 1 ;; esac; [ -n "$1" ]; fi
}
step "Leadership on the node to freeze (${FREEZE_NODE:-any worker})"
for _ in $(seq 1 12); do
  L=$(leader)
  node=$(kn get pod "$L" -o jsonpath='{.spec.nodeName}' 2>/dev/null || true)
  on_target "$node" && break
  info "leader $L runs on $node: handing it over"
  kn delete pod "$L" >/dev/null
  sleep 6
done
on_target "$node" || fail "could not get the leader onto ${FREEZE_NODE:-a worker} (is an Aggregator pod running there?)"
T=$(lease_term)
info "leader $L (term $T) on node $node"

step "Freeze node $node for ${FREEZE_SEC}s"
start_db=$(psql_q "SELECT now()")
if [ -n "${FREEZE_CMD:-}" ]; then
  # whatever happens next, never leave the node frozen
  trap 'eval "$THAW_CMD" >/dev/null 2>&1 || true; cleanup_pf' EXIT
  show "$FREEZE_CMD"
  eval "$FREEZE_CMD"
  frozen_at=$SECONDS
  sleep "$FREEZE_SEC"
  # stay frozen until someone else holds the lease, max +150 s: on a nested-virtualisation laptop the k3s API
  # itself answers slowly while a VM is paused, which delays the takeover (an empty answer is not a takeover)
  wait_until 150 "another instance holding the lease while $node is frozen" sh -c "h=\$(kubectl --context '$KUBE_CONTEXT' -n $NS get lease aggregator-ingest -o jsonpath='{.spec.holderIdentity}' --request-timeout=5s) && [ -n \"\$h\" ] && [ \"\$h\" != '$L' ]"
  info "$node frozen for $((SECONDS - frozen_at)) s"
else
  printf '   >>> suspend the VM of %s NOW, keep it suspended %ss, then press Enter <<<' "$node" "$FREEZE_SEC"
  read -r _
fi
N=$(leader)
NT=$(lease_term)
info "lease while $node is frozen: holder=$N term=$NT"
[ "$N" != "$L" ] || fail "nobody took over (freeze shorter than the lease?)"
ok "$N took over with term $NT > $T"

step "Wake $node up"
if [ -n "${THAW_CMD:-}" ]; then
  show "$THAW_CMD"
  eval "$THAW_CMD"
  trap cleanup_pf EXIT
else
  printf '   >>> resume the VM of %s NOW, then press Enter <<<' "$node"
  read -r _
fi
info "letting the woken instance run for 30 s"
sleep 30

step "What the woken leader did"
kn logs "$L" --since=3m 2>/dev/null |
  grep -E '"msg":"(leader_lost|write_fenced|poll_skipped|fence_rejected|dlq_worker_fenced|ingestion_stopped|hazard_stored|poll_ok)"' |
  jqx -r '"   \(.ts) \(.msg) \(.reason // .error // .source // "")"' | head -15 || info "(pod $L was replaced while frozen)"
late=$(psql_q "SELECT count(*) FROM outbox WHERE writer_term = $T AND stored_at > '$start_db'")
info "rows stored by term $T after the freeze began (store clock): $late"
[ "$late" = 0 ] || fail "the frozen leader stored something after it was replaced"
ok "the woken instance stored nothing: its term $T was fenced by term $(lease_term)"

bash "$ROOT/scripts/demo/m2-single-writer.sh"
