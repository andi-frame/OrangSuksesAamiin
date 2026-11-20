#!/usr/bin/env bash
# Bonus "Kapasitas yang menyesuaikan diri" (make k8s-up first; k3s ships metrics-server). Self-checking.
#   HPA on -> load rises (k6 from this machine, NodePorts) -> replicas go up -> load stops -> replicas go down.
#   HPA_VUS (1500) HPA_HOLD (180s); the HPA is switched off again at the end (P2 tests scale by hand).
. "$(dirname "${BASH_SOURCE[0]}")/k8s-lib.sh"
bash "$ROOT/scripts/k8s/hpa.sh" on
trap 'bash "$ROOT/scripts/k8s/hpa.sh" off >/dev/null 2>&1; cleanup_pf' EXIT
nodes=$(kc get nodes -o jsonpath='{range .items[*]}{.status.addresses[?(@.type=="InternalIP")].address}{" "}{end}')
export CLIENT_API_URLS="${CLIENT_API_URLS:-$(for ip in $nodes; do printf 'http://%s:30080,' "$ip"; done | sed 's/,$//')}"
export AUTH_URL="${AUTH_URL:-http://$(echo "$nodes" | awk '{print $1}'):30084}"
export K6_MODE=scale K6_VUS="${HPA_VUS:-1500}" K6_RAMP=30s K6_HOLD="${HPA_HOLD:-180s}" K6_THINK_S=0.2 K6_SUMMARY=docs/evidence/m2/k6-hpa-summary.json
export K6_CLIENT_ID="$AUTH_LAPANGAN_CLIENT_ID" K6_CLIENT_SECRET="$AUTH_LAPANGAN_CLIENT_SECRET"
replicas() { echo "$(kn get deploy client-api -o jsonpath='{.status.replicas}') $(kn get deploy aggregator -o jsonpath='{.status.replicas}')"; }
read -r api0 agg0 <<<"$(replicas)"

step "Load on (k6 $K6_VUS VUs), HPA watching CPU"
if command -v k6 >/dev/null; then (cd "$ROOT" && k6 run --quiet scripts/loadtest/m2.js >/dev/null 2>&1) &
else docker run --rm --network host -e CLIENT_API_URLS -e AUTH_URL -e K6_MODE -e K6_VUS -e K6_RAMP -e K6_HOLD -e K6_THINK_S \
  -e K6_SUMMARY -e K6_CLIENT_ID -e K6_CLIENT_SECRET -v "$ROOT:/work" -w /work grafana/k6:0.54.0 run --quiet scripts/loadtest/m2.js >/dev/null 2>&1 &
fi
k6pid=$!
api_max=$api0 agg_max=$agg0
while kill -0 "$k6pid" 2>/dev/null; do
  read -r a g <<<"$(replicas)"
  [ "$a" -gt "$api_max" ] && api_max=$a
  [ "$g" -gt "$agg_max" ] && agg_max=$g
  printf '   %s  client-api %s  aggregator %s   %s\n' "$(date +%T)" "$a" "$g" "$(kn get hpa -o jsonpath='{range .items[*]}{.metadata.name}={.status.currentMetrics[0].resource.current.averageUtilization}% {end}')"
  sleep 15
done
info "peak replicas: client-api $api0 -> $api_max, aggregator $agg0 -> $agg_max"
[ "$api_max" -gt "$api0" ] || [ "$agg_max" -gt "$agg0" ] || fail "no scale-up under load (raise HPA_VUS)"
ok "instances added by themselves while the load rose"

step "Load off: replicas go back down by themselves"
down() { read -r a g <<<"$(replicas)"; [ "$a" -lt "$api_max" ] || [ "$g" -lt "$agg_max" ]; }
wait_until 300 "scale-down" down
kn get hpa
ok "instances removed by themselves when the load fell"
printf '\n%sHPA PASS%s\n' "$C_G$C_B" "$C_0"
