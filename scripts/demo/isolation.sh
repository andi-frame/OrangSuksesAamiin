#!/usr/bin/env bash
# P4 evidence: the Canonical Store (PostgreSQL) and the broker (Kafka) are not
# reachable from any container except those that own/use them.
#
# It launches throwaway containers on the SAME networks other services use and
# tries to connect. A service in app-net (mocks, client-facing API, auth ...)
# must not even be able to RESOLVE the database name.
. "$(dirname "$0")/../lib.sh"
require_stack

PROJECT="${COMPOSE_PROJECT_NAME:-bnpb}"
APP="${PROJECT}_app-net"; CAN="${PROJECT}_canonical-net"; BRK="${PROJECT}_broker-net"
IMG=busybox:1.36

probe() { # probe NETWORK HOST PORT  -> prints result, returns 0 when reachable
  docker run --rm --network "$1" "$IMG" sh -c "nc -z -w 3 $2 $3" >/dev/null 2>&1
}
members() { docker network inspect "$1" -f '{{range .Containers}}{{.Name}} {{end}}' | tr ' ' '\n' | sed '/^$/d' | sed "s/^${PROJECT}-//; s/-1$//" | sort | tr '\n' ' '; }

step "Who is attached to which network (docker network inspect)"
for n in "$APP" "$CAN" "$BRK"; do info "$n: $(members "$n")"; done

step "PostgreSQL (Canonical Store) is reachable ONLY from the Aggregator's network"
info "networks the database container is attached to: $(docker inspect -f '{{range $k,$v := .NetworkSettings.Networks}}{{$k}} {{end}}' "$(docker compose ps -q postgres-canonical)")"
if [ "$(members "$CAN")" = "aggregator postgres-canonical " ]; then
  ok "canonical-net contains exactly: aggregator + postgres-canonical"
else
  fail "canonical-net has unexpected members: $(members "$CAN")"
fi
if [ -z "$(docker port "$(docker compose ps -q postgres-canonical)" 2>/dev/null)" ]; then ok "postgres publishes no port to the host"; else fail "postgres publishes a port"; fi
if probe "$APP" postgres-canonical 5432; then fail "a container in app-net CAN reach postgres-canonical:5432"; else ok "container on app-net (where mocks / client-facing API live): postgres-canonical:5432 is unreachable"; fi
if probe "$BRK" postgres-canonical 5432; then fail "a container in broker-net CAN reach postgres"; else ok "container on broker-net (consumers): postgres-canonical:5432 is unreachable"; fi
if probe "$CAN" postgres-canonical 5432; then ok "control: a container on canonical-net (i.e. the Aggregator's network) does reach it"; else warn "control probe failed (database not ready?)"; fi

step "Kafka is reachable only from broker-net (producer + consumers), not from app-net"
if probe "$APP" kafka 9092; then fail "a container in app-net CAN reach kafka:9092"; else ok "container on app-net: kafka:9092 is unreachable"; fi
if probe "$BRK" kafka 9092; then ok "control: a container on broker-net does reach kafka:9092"; else warn "control probe failed (kafka not ready?)"; fi

step "Other components get hazards only through the Aggregator API"
if [ "$(code "${AGG_URL}/internal/hazards")" = "401" ] && [ "$(code -H 'X-Service-Key: nope' "${AGG_URL}/internal/hazards")" = "403" ]; then
  ok "the Aggregator API demands its service key (401 without, 403 with a wrong one)"
else
  fail "Aggregator API is not protected"
fi
printf '\n%sISOLATION PASS%s\n' "$C_G$C_B" "$C_0"
