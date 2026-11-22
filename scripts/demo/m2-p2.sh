#!/usr/bin/env bash
# M2 Problem 2 on the running cluster (make k8s-up first). Self-checking: exits != 0 on failure.
#   1. >= 3 Aggregator instances behind one Service, spread over the nodes, only Ready ones routed
#   2. one leader; BMKG / PVMBG are polled by that instance only (upstream request logs, field "caller")
#   3. no duplicate HazardEvent in the store
#   4. the leader is killed (no graceful release): another instance takes over <= 60 s, no gap, no duplicate
#   5. fencing audit: per leadership term, the store's own clock shows non-overlapping write periods
#   6. the Aggregator version is replaced under load without a single failed request
# NEW_TAG=<tag> (built with: IMAGE_TAG=<tag> bash scripts/k8s/images.sh aggregator) switches to that version in
# step 6; without it the same version is rolled (same mechanism).
. "$(dirname "${BASH_SOURCE[0]}")/k8s-lib.sh"

kn rollout status deploy/aggregator --timeout=180s >/dev/null
wait_until 180 "rollout settled: no terminating Aggregator, the lease on a running one" settled
forward aggregator 18083 8083
forward bmkg-mock 18081 8081

step "Aggregator instances, nodes and the Service in front of them"
show "kubectl -n $NS get pods -l app=aggregator -o wide"
kn get pods -l app=aggregator -o wide
n_pods=$(kn get pods -l app=aggregator --field-selector=status.phase=Running -o name | wc -l | tr -d ' ')
n_nodes=$(kn get pods -l app=aggregator -o jsonpath='{range .items[*]}{.spec.nodeName}{"\n"}{end}' | sort -u | wc -l | tr -d ' ')
schedulable=$(kc get nodes -o jsonpath='{range .items[*]}{.metadata.name} {.spec.taints[*].effect}{"\n"}{end}' | grep -vc NoSchedule || true)
[ "$n_pods" -ge 3 ] || fail "only $n_pods aggregator pods running"
ok "$n_pods instances on $n_nodes node(s) (schedulable nodes: $schedulable; topologySpreadConstraints maxSkew=1)"
[ "$n_nodes" -ge "$((schedulable < 2 ? schedulable : 2))" ] || fail "instances are not spread over the nodes"
show "kubectl -n $NS get endpointslices -l kubernetes.io/service-name=aggregator"
kn get endpointslices -l kubernetes.io/service-name=aggregator -o jsonpath='{range .items[*].endpoints[*]}   {.targetRef.name} ready={.conditions.ready}{"\n"}{end}'
info "readinessProbe /ready every 2 s, 2 failures => out of the Service (deploy/k8s/aggregator.yaml)"

step "Exactly one leader"
L=$(leader)
T=$(lease_term)
[ -n "$L" ] || fail "no lease holder"
info "Lease aggregator-ingest: holder=$L term=$T"
leaders=0
for p in $(kn get pods -l app=aggregator -o jsonpath='{.items[*].metadata.name}'); do
  st=$(kn exec "$p" -- wget -qO- --header "X-Service-Key: $AGGREGATOR_SERVICE_KEY" http://127.0.0.1:8083/internal/leader)
  is=$(echo "$st" | jqx -r '.instance.leader')
  info "$p leader=$is term=$(echo "$st" | jqx -r '.instance.term')"
  [ "$is" = true ] && leaders=$((leaders + 1))
done
[ "$leaders" -eq 1 ] || fail "$leaders instances believe they lead"
ok "exactly one instance leads ($L)"

step "Each upstream is polled by one instance at a time (request logs of the mocks, last 10 min)"
for mock in bmkg-mock pvmbg-mock; do
  show "kubectl -n $NS logs deploy/$mock --since=10m | jq: per caller, first and last data request"
  periods=$(upstream_callers "$mock" 10m)
  echo "$periods" | awk '{printf "   %-32s %4s requests  %s .. %s\n", $4, $3, $5, $6}'
  [ "$(echo "$periods" | overlapping)" = 0 ] || fail "$mock was polled by two instances in the same period"
  echo "$periods" | tail -1 | grep -q "$L" || fail "$mock is not polled by the current leader"
done
ok "callers only follow each other (one per leadership term), never overlap; the current one is the leader"

dup_check() {
  duplicate_hazards
}
step "No duplicate HazardEvent in the store"
show "psql: SELECT count(*), count(DISTINCT (source, source_ref_id)) FROM hazards"
psql_table "SELECT count(*) AS hazards, count(DISTINCT (source, source_ref_id)) AS distinct_records FROM hazards"
[ "$(dup_check)" = 0 ] || fail "duplicates in the store"
ok "no duplicates"

step "Delete the leader pod (what Tester Agent kill-leader does); another instance must take over <= 60 s"
info "a pod that gets SIGTERM hands the lease back (takeover in ~2 s); one that dies without it (node lost,"
info "VM frozen) is replaced once the lease is not renewed for 15 s - see scripts/demo/m2-freeze.sh"
bmkg_admin /admin/generate '{"magnitude":5.4}' >/dev/null
before=$(psql_sum "SELECT count(*) FROM hazards")
show "kubectl -n $NS delete pod $L --grace-period=0 --force"
t0=$SECONDS
kn delete pod "$L" --grace-period=0 --force >/dev/null 2>&1
# events that happen while nobody leads must not be lost
for i in 1 2 3; do
  bmkg_admin /admin/generate "{\"magnitude\":5.$i}" >/dev/null || true
  sleep 3
done
wait_until 90 "a new lease holder" sh -c "[ -n \"\$(kubectl --context '$KUBE_CONTEXT' -n $NS get lease aggregator-ingest -o jsonpath='{.spec.holderIdentity}')\" ] && [ \"\$(kubectl --context '$KUBE_CONTEXT' -n $NS get lease aggregator-ingest -o jsonpath='{.spec.holderIdentity}')\" != '$L' ]"
N=$(leader)
taken=$((SECONDS - t0))
info "new leader $N (term $(lease_term)) after ${taken}s"
wait_until 60 "a successful poll by $N" sh -c "kubectl --context '$KUBE_CONTEXT' -n $NS logs $N --since=90s | grep -q '\"msg\":\"poll_ok\".*\"source\":\"BMKG\"'"
resumed=$((SECONDS - t0))
[ "$resumed" -le 60 ] || fail "ingestion resumed after ${resumed}s (> 60 s)"
ok "ingestion resumed by $N ${resumed}s after the kill (lease duration 15 s)"
kn logs "$N" --since=90s | grep -E '"msg":"(lease_taken_over|leader_acquired|fence_raised|ingestion_started)"' | head -5 | sed 's/^/   /'

step "No gap: every BMKG event the mock produced is in the store"
sleep 6
ensure_forwards
# Judge the failover window: every event the mock produced from ${GAP_WINDOW_MIN:-15} min before the kill on. A mock
# that restarted creates seed events backdated up to 36 h, older than the stored polling cursor; those are a known
# limitation of a time cursor (report P2 §6), not a gap caused by the leader change.
since_iso=$(date -u -d "-${GAP_WINDOW_MIN:-15} minutes" +%FT%TZ 2>/dev/null || date -u -v-"${GAP_WINDOW_MIN:-15}"M +%FT%TZ)
mock_ids=$(curl -sS --max-time 15 -H "X-BMKG-Key: $BMKG_API_KEY" "$BMKG/seismic-events" |
  jqx -r --arg since "$since_iso" '.[] | select(.occurred_at[0:19] >= $since[0:19]) | .event_id' | sort)
[ -n "$mock_ids" ] || fail "could not read the BMKG mock's events"
db_ids=$(psql_q "SELECT source_ref_id FROM hazards WHERE source='BMKG' ORDER BY 1" | sort)
missing=$(comm -23 <(echo "$mock_ids") <(echo "$db_ids") | grep -c . || true)
info "mock events since $since_iso: $(echo "$mock_ids" | grep -c .), stored: $(echo "$db_ids" | grep -c .), missing: $missing; hazards before kill: $before, now: $(psql_sum 'SELECT count(*) FROM hazards')"
[ "$missing" = 0 ] || fail "$missing events never stored"
[ "$(dup_check)" = 0 ] || fail "duplicates after failover"
ok "no gap and no duplicate across the failover"

step "Fencing audit: write periods per leadership term (store clock)"
show "psql: SELECT writer_term, writer, count(*), min(stored_at), max(stored_at) FROM outbox GROUP BY 1,2"
term_periods
overlaps=$(term_overlaps)
psql_table "SELECT name AS lease, term AS fenced_term, holder, raised_at FROM fence"
[ "$overlaps" = 0 ] || fail "$overlaps pairs of terms wrote in overlapping periods"
ok "no two terms ever wrote in the same period (the store rejects an older term once a newer one wrote)"

step "Replace the Aggregator version under load: zero failed requests"
kn delete pod loadgen --ignore-not-found >/dev/null 2>&1
kn run loadgen --image=curlimages/curl:8.11.1 --restart=Never --labels=role=other,app=loadgen \
  --overrides='{"spec":{"containers":[{"name":"loadgen","image":"curlimages/curl:8.11.1","resources":{"requests":{"memory":"64Mi"},"limits":{"memory":"128Mi"}},"command":["sh","-c","end=$(( $(date +%s) + 75 )); for w in 1 2 3 4; do ( while [ $(date +%s) -lt $end ]; do curl -s -o /dev/null -w \"%{http_code}\\n\" --max-time 5 -H \"X-Service-Key: '"$AGGREGATOR_SERVICE_KEY"'\" http://aggregator:8083/internal/hazards?limit=20; done ) & done; wait"]}]}}' >/dev/null
wait_until 60 "loadgen running" sh -c "[ \"\$(kubectl --context '$KUBE_CONTEXT' -n $NS get pod loadgen -o jsonpath='{.status.phase}')\" = Running ]"
sleep 5
old_version=$(curl -s "$AGG/health" | jqx -r .version)
if [ -n "${NEW_TAG:-}" ]; then
  show "kubectl -n $NS set image deploy/aggregator aggregator=${IMAGE_PREFIX}argus/aggregator:$NEW_TAG"
  kn set image deploy/aggregator "aggregator=${IMAGE_PREFIX}argus/aggregator:$NEW_TAG" >/dev/null
else
  show "kubectl -n $NS rollout restart deploy/aggregator   (set NEW_TAG=<tag> to switch versions)"
  kn rollout restart deploy/aggregator >/dev/null
fi
kn rollout status deploy/aggregator --timeout=180s
wait_until 120 "loadgen finished" sh -c "[ \"\$(kubectl --context '$KUBE_CONTEXT' -n $NS get pod loadgen -o jsonpath='{.status.phase}')\" = Succeeded ]"
codes=$(kn logs loadgen | sort | uniq -c)
kn delete pod loadgen >/dev/null 2>&1 || true
echo "$codes" | sed 's/^/   HTTP /'
total=$(echo "$codes" | awk '{s+=$1} END {print s+0}')
failed=$(echo "$codes" | awk '$2 != "200" {s+=$1} END {print s+0}')
ensure_forwards
info "version before: $old_version, image after: $(kn get deploy aggregator -o jsonpath="{.spec.template.spec.containers[0].image}") (pods: $(kn get pods -l app=aggregator -o jsonpath="{.items[*].spec.containers[0].image}" | tr " " "
" | sort -u | tr "
" " ")); requests: $total, failed: $failed"
[ "$total" -gt 100 ] || fail "too few requests to judge ($total)"
[ "$failed" = 0 ] || fail "$failed requests failed during the rollout"
ok "rolling update under load without a failed request"
info "leader after the rollout: $(leader) (term $(lease_term)); the old leader released the lease on SIGTERM"

printf '\n%sM2 P2 PASS%s\n' "$C_G$C_B" "$C_0"
