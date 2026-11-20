#!/usr/bin/env bash
# Create one Secret per component from .env. Each component receives only its own credentials:
# the BMKG key is not in the PVMBG mock, the database password is only in the store and the Aggregator, ...
. "$(dirname "${BASH_SOURCE[0]}")/common.sh"
load_env

need() { for v in "$@"; do [ -n "${!v:-}" ] || die "$v is empty in .env"; done; }

# generated(): credentials WE choose (not given by anyone) are created once, randomly, into .env
# instead of having a default in the code: PANTAI_AUTH_TOKEN (we run the sensor), GRAFANA_ADMIN_PASSWORD.
generated() {
  for v in "$@"; do
    if [ -z "${!v:-}" ]; then
      val="$(head -c 24 /dev/urandom | od -An -tx1 | tr -d ' \n')"
      if grep -q "^$v=" "$ENV_FILE"; then
        sed -i.bak "s|^$v=.*|$v=$val|" "$ENV_FILE" && rm -f "$ENV_FILE.bak"
      else
        printf '%s=%s\n' "$v" "$val" >>"$ENV_FILE"
      fi
      printf -v "$v" '%s' "$val"
      export "${v?}"
      echo "   generated $v into $ENV_FILE"
    fi
  done
}
generated PANTAI_AUTH_TOKEN GRAFANA_ADMIN_PASSWORD
need BMKG_API_KEY BMKG_ADMIN_TOKEN PVMBG_TOKEN_SECRET PVMBG_ADMIN_TOKEN PVMBG_GATEWAY_TOKEN \
  AGGREGATOR_SERVICE_KEY AGGREGATOR_OPS_TOKEN AUDIT_TOKEN CANONICAL_DB_PASSWORD AUTH_JWT_SECRET \
  AUTH_MEDIA_CLIENT_ID AUTH_MEDIA_CLIENT_SECRET AUTH_LAPANGAN_CLIENT_ID AUTH_LAPANGAN_CLIENT_SECRET \
  AUTH_PUSAT_CLIENT_ID AUTH_PUSAT_CLIENT_SECRET PANTAI_AUTH_TOKEN GRAFANA_ADMIN_PASSWORD

db_user="${CANONICAL_DB_USER:-canonical}" db_name="${CANONICAL_DB_NAME:-canonical}"

secret() { # name key=value ...
  local name=$1
  shift
  local args=()
  for kv in "$@"; do args+=("--from-literal=$kv"); done
  kn create secret generic "$name" "${args[@]}" --dry-run=client -o yaml | kn apply -f - >/dev/null
  echo "   secret/$name (${#args[@]} keys)"
}

say "secrets in $NS"
secret bmkg-mock-credentials "BMKG_API_KEY=$BMKG_API_KEY" "BMKG_ADMIN_TOKEN=$BMKG_ADMIN_TOKEN"
secret pvmbg-mock-credentials "PVMBG_TOKEN_SECRET=$PVMBG_TOKEN_SECRET" "PVMBG_ADMIN_TOKEN=$PVMBG_ADMIN_TOKEN"
secret canonical-db-credentials "POSTGRES_USER=$db_user" "POSTGRES_PASSWORD=$CANONICAL_DB_PASSWORD" "POSTGRES_DB=$db_name"
shard_env=()
if [ "$K8S_STORAGE" = sharded ]; then
  need CANONICAL_DB_ADMIN_PASSWORD CANONICAL_DB_REPLICATION_PASSWORD
  secret canonical-shard-credentials "POSTGRES_USER=$db_user" "POSTGRES_PASSWORD=$CANONICAL_DB_PASSWORD" \
    "POSTGRES_DB=$db_name" "POSTGRES_ADMIN_PASSWORD=$CANONICAL_DB_ADMIN_PASSWORD" \
    "POSTGRES_REPLICATION_PASSWORD=$CANONICAL_DB_REPLICATION_PASSWORD"
  urls=()
  for shard in shard-0 shard-1 shard-2; do
    urls+=("postgres://$db_user:$CANONICAL_DB_PASSWORD@$shard:5432/$db_name?sslmode=disable&connect_timeout=2&target_session_attrs=read-write")
  done
  shard_env=("CANONICAL_SHARD_URLS=$(IFS=,; echo "${urls[*]}")")
fi
secret aggregator-credentials ${shard_env[@]+"${shard_env[@]}"} \
  "AGGREGATOR_SERVICE_KEY=$AGGREGATOR_SERVICE_KEY" "AGGREGATOR_OPS_TOKEN=$AGGREGATOR_OPS_TOKEN" "AUDIT_TOKEN=$AUDIT_TOKEN" \
  "BMKG_API_KEY=$BMKG_API_KEY" "PVMBG_GATEWAY_TOKEN=$PVMBG_GATEWAY_TOKEN" "PANTAI_AUTH_TOKEN=$PANTAI_AUTH_TOKEN" \
  "CANONICAL_DB_URL=postgres://$db_user:$CANONICAL_DB_PASSWORD@postgres-canonical:5432/$db_name?sslmode=disable"
secret auth-service-credentials "AUTH_JWT_SECRET=$AUTH_JWT_SECRET" \
  "AUTH_MEDIA_CLIENT_ID=$AUTH_MEDIA_CLIENT_ID" "AUTH_MEDIA_CLIENT_SECRET=$AUTH_MEDIA_CLIENT_SECRET" \
  "AUTH_LAPANGAN_CLIENT_ID=$AUTH_LAPANGAN_CLIENT_ID" "AUTH_LAPANGAN_CLIENT_SECRET=$AUTH_LAPANGAN_CLIENT_SECRET" \
  "AUTH_PUSAT_CLIENT_ID=$AUTH_PUSAT_CLIENT_ID" "AUTH_PUSAT_CLIENT_SECRET=$AUTH_PUSAT_CLIENT_SECRET"
secret client-api-credentials "AGGREGATOR_SERVICE_KEY=$AGGREGATOR_SERVICE_KEY" "AUTH_JWT_SECRET=$AUTH_JWT_SECRET"
# Sensor Pantai (P5): the name and key are fixed by the assistants' manifest (secretKeyRef pantai-token/token);
# the Aggregator gets the same token in its own Secret (above: PANTAI_AUTH_TOKEN)
secret pantai-token "token=$PANTAI_AUTH_TOKEN"
# Grafana admin (observability); the dashboards are also readable anonymously (Viewer)
secret grafana-credentials "GF_SECURITY_ADMIN_USER=${GRAFANA_ADMIN_USER:-admin}" "GF_SECURITY_ADMIN_PASSWORD=$GRAFANA_ADMIN_PASSWORD"
# Feed Satelit (P4): only the component that reads the feed mounts this one
secret satellite-feed-credentials "SATELLITE_FEED_TOKEN=${SATELLITE_FEED_TOKEN:-}" \
  "SATELLITE_FEED_URL=${SATELLITE_FEED_URL:-https://satellite.naufarrel.dev}"
