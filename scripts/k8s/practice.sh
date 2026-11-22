#!/usr/bin/env bash
# Run one Tester Agent practice scenario and keep its log as evidence.
#   bash scripts/k8s/practice.sh            -> list scenarios
#   bash scripts/k8s/practice.sh clock-skew -> run it, then check the single-writer guarantees (P2 scenarios)
. "$(dirname "${BASH_SOURCE[0]}")/common.sh"
# MSYS_NO_PATHCONV: Git Bash on Windows would rewrite "/app" into a Windows path
if [ $# -eq 0 ]; then
  MSYS_NO_PATHCONV=1 kc exec -n tester-agent daemonset/tester-agent -- /app list
  exit 0
fi
mkdir -p "$ROOT/docs/evidence/m2"
out="$ROOT/docs/evidence/m2/tester-$1.log"
say "practice scenario $1 (log: ${out#"$ROOT"/})"
# new log lines are found by line offset per agent pod, not by time: clock-skew shifts the
# node clocks, so the agent's timestamps can jump backwards and a --since-time filter would hide them
pods=$(kc get pods -n tester-agent -l app=tester-agent -o name)
declare -A base=()
for p in $pods; do base[$p]=$(kc logs -n tester-agent "$p" 2>/dev/null | wc -l); done
new_logs() { for p in $pods; do kc logs -n tester-agent "$p" 2>/dev/null | tail -n +$((base[$p] + 1)) | sed "s|^|[${p#pod/}] |"; done; }
# run it from the agent on the control-plane node: a scenario that powers a worker off must not cut its own exec
cp_node=$(kc get nodes -l node-role.kubernetes.io/control-plane -o jsonpath='{.items[0].metadata.name}')
agent=$(kc get pods -n tester-agent -l app=tester-agent --field-selector "spec.nodeName=$cp_node" -o jsonpath='{.items[0].metadata.name}')
MSYS_NO_PATHCONV=1 kc exec -n tester-agent "${agent:-daemonset/tester-agent}" -- /app run practice "$1" 2>&1 | tee "$out"
# the agent runs the scenario asynchronously; its log ends with "pemulihan akhir selesai" (final recovery done)
say "waiting for the scenario to finish (max ${PRACTICE_TIMEOUT_SEC:-900} s)"
deadline=$((SECONDS + ${PRACTICE_TIMEOUT_SEC:-900}))
until new_logs | grep -q 'pemulihan akhir selesai'; do
  [ $SECONDS -ge $deadline ] && { echo "scenario did not finish in time" | tee -a "$out"; break; }
  sleep 5
done
say "Tester Agent log (all nodes)"
new_logs | tee -a "$out"
case "$1" in
  clock-skew | kill-leader | partition)
    say "letting the system recover for ${SETTLE_SEC:-45} s, then checking the single-writer guarantees"
    sleep "${SETTLE_SEC:-45}"
    SINCE=10m bash "$ROOT/scripts/demo/m2-single-writer.sh" 2>&1 | tee -a "$out"
    ;;
esac
