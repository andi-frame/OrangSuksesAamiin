#!/usr/bin/env bash
# Run the Postgres adapter against the repotest contract on a throwaway container.
# Usage: bash scripts/test-postgres.sh   (or: make test-postgres)
set -euo pipefail
cd "$(dirname "$0")/.."
. scripts/winenv.sh
name="pg-contract-$$"
port="${TEST_PG_PORT:-55432}"
docker run -d --rm --name "$name" -e POSTGRES_PASSWORD=test -e POSTGRES_DB=contract \
  -p "127.0.0.1:$port:5432" postgres:16-alpine >/dev/null
trap 'docker stop "$name" >/dev/null 2>&1 || true' EXIT
for _ in $(seq 1 40); do
  docker exec "$name" pg_isready -U postgres -d contract >/dev/null 2>&1 && break
  sleep 1
done
sleep 1
cd services/aggregator
TEST_CANONICAL_DB_URL="postgres://postgres:test@127.0.0.1:$port/contract?sslmode=disable" \
  GOWORK=off go test -count=1 -race -v ./internal/adapters/postgres/ "$@"
