#!/usr/bin/env bash
# M2 Problem 1 criteria 4-6 on the running cluster (sharded storage). Self-checking.
#   1. before: every shard has a primary and a synchronous replica on a different node; primaries on > 1 node
#   2. reference: every active (source, source_ref_id) through the Audit API, and the checksums
#   3. a Tester Agent scenario takes a worker away (SCENARIO, default wipe-node: node off + its data deleted)
#      while a reader asks for the 50 newest events every 0.5 s: every answer must be 200 (complete, or
#      partial=true with unavailable_units), never 5xx and never hanging (curl --max-time 5)
#   4. recovery without operator: every shard back to 2 members, replica synchronous, preferred primaries back
#      (the returning node takes write load again: balance-shard)
#   5. no HazardEvent lost: every reference record is still there (Audit API, compared record by record)
#   6. SECOND_LOSS=1: take the worker away again and check the reads again (survives the next loss)
#   Env: SCENARIO (wipe-node) SECOND_LOSS (1) RECOVER_TIMEOUT_S (900) API (http://127.0.0.1:30080) AGG_URL (:30083)
. "$(dirname "${BASH_SOURCE[0]}")/k8s-lib.sh"
export MSYS_NO_PATHCONV=1   # Git Bash: keep /tmp/patroni.yaml and /usr/local/bin paths for kubectl exec as they are
[ "$K8S_STORAGE" = sharded ] || fail "select K8S_STORAGE=sharded"
SCENARIO="${SCENARIO:-wipe-node}" SECOND_LOSS="${SECOND_LOSS:-1}" RECOVER_TIMEOUT_S="${RECOVER_TIMEOUT_S:-900}"
API="${API:-http://127.0.0.1:30080}" AGG_URL="${AGG_URL:-http://127.0.0.1:30083}" AUTHU="${AUTHU:-http://127.0.0.1:30084}"
evidence="$ROOT/docs/evidence/m2/p1-recovery-latest.log"
work=$(mktemp -d)
trap '[ -n "${reader:-}" ] && { pkill -P "$reader" 2>/dev/null; kill "$reader" 2>/dev/null; }; rm -rf "$work"; cleanup_pf' EXIT
exec > >(tee "$evidence") 2>&1

placement() { kn get pods -l app=canonical-shard -o wide -L shard,db-role --no-headers |
  awk '{printf "   %-10s %-8s %-8s %-8s %s\n", $1, $2, $(NF-1), $NF, $7}'; }
# shard -> "primary-node replica-node" (only members that are Ready)
shard_nodes() {
  kn get pods -l "app=canonical-shard,shard=$1" -o jsonpath='{range .items[*]}{.metadata.labels.db-role}{" "}{.spec.nodeName}{" "}{.status.containerStatuses[0].ready}{"\n"}{end}'
}
healthy() { # every shard: one ready primary and one ready replica on another node, replica synchronous
  local s rows p r
  for s in shard-0 shard-1 shard-2; do
    rows=$(shard_nodes "$s")
    p=$(echo "$rows" | awk '$1=="primary" && $3=="true" {print $2}')
    r=$(echo "$rows" | awk '$1=="replica" && $3=="true" {print $2}')
    [ -n "$p" ] && [ -n "$r" ] && [ "$p" != "$r" ] || return 1
    [ "$(kn exec "$s-0" -c postgres -- patronictl -c /tmp/patroni.yaml list -f json 2>/dev/null |
      jqx '[.[] | select(.Role=="Sync Standby" and .State=="streaming")] | length' 2>/dev/null || echo 0)" = 1 ] ||
      [ "$(kn exec "$s-1" -c postgres -- patronictl -c /tmp/patroni.yaml list -f json 2>/dev/null |
        jqx '[.[] | select(.Role=="Sync Standby" and .State=="streaming")] | length' 2>/dev/null || echo 0)" = 1 ] || return 1
  done
}
primary_nodes() { kn get pods -l app=canonical-shard,db-role=primary -o jsonpath='{range .items[*]}{.spec.nodeName}{"\n"}{end}' | sort -u | wc -l | tr -d ' '; }
preferred_ok() { # every shard's primary on its preferred node (ConfigMap shard-placement)
  local s want have
  for s in shard-0 shard-1 shard-2; do
    want=$(kn get configmap shard-placement -o jsonpath="{.data.$s}")
    have=$(shard_nodes "$s" | awk '$1=="primary" && $3=="true" {print $2}')
    [ "$want" = "$have" ] || return 1
  done
}
audit_refs() { # all active (source, ref) through the Audit API, paged
  local cursor="" page q
  : >"$1"
  while :; do
    q="/internal/audit/events?limit=1000${cursor:+&cursor=$cursor}"
    page=$(curl -sS --max-time 30 -H "Authorization: Bearer $AUDIT_TOKEN" "$AGG_URL$q") || return 1
    echo "$page" | jqx -r '.items[] | "\(.source)|\(.source_ref_id)"' >>"$1"
    [ "$(echo "$page" | jqx -r '.partial')" = false ] || { echo "partial page" >&2; return 1; }
    cursor=$(echo "$page" | jqx -r '.next_cursor // empty' | sed 's/=/%3D/g')
    [ -n "$cursor" ] || break
  done
  sort -o "$1" "$1"
}
token=$(curl -sS --max-time 10 "$AUTHU/v1/token" -H 'Content-Type: application/json' \
  -d "{\"client_id\":\"$AUTH_LAPANGAN_CLIENT_ID\",\"client_secret\":\"$AUTH_LAPANGAN_CLIENT_SECRET\"}" | jqx -r .access_token)
[ -n "$token" ] && [ "$token" != null ] || fail "no Tim Lapangan token from $AUTHU"
read_loop() { # one line per request: epoch http_code time_total partial unavailable_units
  while [ -d "$work" ]; do
    curl -s -o "$work/body" -w '%{http_code} %{time_total}' --max-time 5 -H "Authorization: Bearer $token" \
      "$API/v1/hazards?limit=50" >"$work/meta" 2>/dev/null || true
    printf '%s %s %s\n' "$(date +%s)" "$(cat "$work/meta" 2>/dev/null || echo '000 5')" \
      "$(jqx -c '[.partial // false, (.unavailable_units // [])]' <"$work/body" 2>/dev/null || echo '[null,[]]')" >>"$1"
    sleep 0.5
  done
}
summarize_reads() {
  awk '{n++; c[$2]++; if ($3+0 > max) max=$3+0; if ($4 ~ /true/) partial++}
       END {printf "   requests %d, by code:", n; for (k in c) printf " %s=%d", k, c[k];
            printf ", partial answers %d, slowest %.3f s\n", partial, max}' "$1"
  local bad
  bad=$(awk '$2 !~ /^200$/' "$1" | wc -l | tr -d ' ')
  [ "$bad" = 0 ] || { awk '$2 !~ /^200$/' "$1" | head -5; fail "$bad requests were not answered 200 within 5 s"; }
  ok "every request answered 200 within 5 s (complete, or partial with a marker)"
}
lose_worker() { # $1: reads file
  read_loop "$1" &
  reader=$!
  bash "$ROOT/scripts/k8s/practice.sh" "$SCENARIO" | sed 's/^/   tester: /' | tail -n 25 || true
  # A powered-off worker comes back with its disk wiped (criterion 5: "node yang sama dengan disk kosong"); starting the
  # VM is the one thing that cannot be done from inside the cluster. Everything after that happens without an operator.
  local down
  for down in $(kc get nodes --no-headers | awk '$2 != "Ready" {print $1}'); do
    info "$down is down; starting it again (NODE_START_CMD)"
    eval "${NODE_START_CMD:-\"/c/Program Files/Oracle/VirtualBox/VBoxManage.exe\" startvm \$down --type headless}" || true
  done
  info "waiting until the storage is redundant again (max ${RECOVER_TIMEOUT_S}s, no operator action)"
  local t0=$SECONDS
  until healthy; do
    [ $((SECONDS - t0)) -lt "$RECOVER_TIMEOUT_S" ] || { placement; fail "storage not redundant after ${RECOVER_TIMEOUT_S}s"; }
    sleep 5
  done
  info "redundant again after $((SECONDS - t0)) s"
  until preferred_ok; do
    [ $((SECONDS - t0)) -lt "$RECOVER_TIMEOUT_S" ] || { placement; fail "preferred primaries not restored"; }
    sleep 5
  done
  info "preferred primaries restored after $((SECONDS - t0)) s (balance-shard switchover)"
  sleep 5
  kill "$reader" 2>/dev/null || true
  reader=
  summarize_reads "$1"
}

step "1. Placement before the loss (criterion 1 and 4: replica on another node, primaries on > 1 node)"
wait_until 300 "storage redundant" healthy
placement
[ "$(primary_nodes)" -ge 2 ] || fail "all primaries on one node"
ok "every shard: ready primary + synchronous replica on a different node; primaries on $(primary_nodes) nodes"

step "2. Reference: every active HazardEvent (Audit API) and checksums"
audit_refs "$work/before" || fail "could not read the Audit API"
info "$(wc -l <"$work/before" | tr -d ' ') active records"
for src in BMKG PVMBG SATELIT SENSOR_PANTAI; do
  curl -sS --max-time 15 -H "Authorization: Bearer $AUDIT_TOKEN" "$AGG_URL/internal/audit/checksum?source=$src" |
    jqx -c '{source, count, checksum, partial}'
done

step "3. Tester Agent '$SCENARIO' while reading the 50 newest events every 0.5 s"
lose_worker "$work/reads1"
placement

step "5. No HazardEvent lost (every reference record still present, Audit API)"
audit_refs "$work/after" || fail "could not read the Audit API"
missing=$(comm -23 "$work/before" "$work/after" | wc -l | tr -d ' ')
info "before $(wc -l <"$work/before" | tr -d ' '), after $(wc -l <"$work/after" | tr -d ' ') (new ingest keeps adding), missing $missing"
[ "$missing" = 0 ] || { comm -23 "$work/before" "$work/after" | head; fail "$missing records lost"; }
ok "0 HazardEvents lost"

if [ "$SECOND_LOSS" = 1 ]; then
  step "6. The next loss after recovery (criterion 6)"
  lose_worker "$work/reads2"
  audit_refs "$work/after2" || fail "could not read the Audit API"
  missing=$(comm -23 "$work/before" "$work/after2" | wc -l | tr -d ' ')
  [ "$missing" = 0 ] || fail "$missing records lost after the second loss"
  ok "second loss survived, 0 HazardEvents lost"
  placement
fi
