#!/usr/bin/env bash
# M2: "one storage, one owner" enforced by the platform. From inside real pods, try to open a TCP
# connection to the Canonical Store and the broker; only their owners / clients get through.
. "$(dirname "${BASH_SOURCE[0]}")/k8s-lib.sh"

try() { # <app> <host> <port> -> "open" | "blocked"
  local pod
  pod=$(kn get pods -l "app=$1" --field-selector=status.phase=Running -o jsonpath='{.items[0].metadata.name}')
  if kn exec "$pod" -- nc -z -w 3 "$2" "$3" >/dev/null 2>&1; then echo open; else echo blocked; fi
}
expect() { # <app> <host> <port> <open|blocked>
  local got
  got=$(try "$1" "$2" "$3")
  info "$(printf '%-20s -> %-20s %s' "$1" "$2:$3" "$got")"
  [ "$got" = "$4" ] || fail "$1 -> $2:$3 is $got, expected $4"
}

step "NetworkPolicies in force"
kn get networkpolicy
step "Canonical Store: Aggregator only"
stores=(postgres-canonical)
if [ "$K8S_STORAGE" = sharded ]; then stores=(shard-0 shard-1 shard-2); fi
for store in "${stores[@]}"; do
  expect aggregator "$store" 5432 open
  for app in client-api auth-service bmkg-mock pvmbg-mock consumer-dashboard; do
    expect "$app" "$store" 5432 blocked
  done
done
ok "only the Aggregator reaches the Canonical Store"

step "Broker (kafka:9092): producer and consumers only"
expect aggregator kafka 9092 open
expect consumer-notifier kafka 9092 open
for app in client-api auth-service pvmbg-mock; do
  expect "$app" kafka 9092 blocked
done
ok "the broker only accepts its clients"
step "Credentials: the store password is only in the store and the Aggregator"
for s in $(kn get secrets -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}' | grep credentials); do
  keys=$(kn get secret "$s" -o jsonpath='{.data}' | jqx -r 'keys | join(",")')
  info "$(printf '%-28s %s' "$s" "$keys")"
done
printf '\n%sM2 isolation PASS%s\n' "$C_G$C_B" "$C_0"
