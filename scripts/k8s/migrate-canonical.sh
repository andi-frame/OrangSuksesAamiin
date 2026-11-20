#!/usr/bin/env bash
# Called by k8s-up with MIGRATE_CANONICAL=1 during the singleton cutover.
. "$(dirname "${BASH_SOURCE[0]}")/common.sh"
[ "${MIGRATE_CANONICAL:-0}" = 1 ] && [ "$K8S_STORAGE" = sharded ] || die "select MIGRATE_CANONICAL=1 and K8S_STORAGE=sharded"
replicas=$(kn get deployment aggregator -o jsonpath='{.spec.replicas}')
[ -n "$replicas" ] || die "could not determine the legacy Aggregator replica count"

say "start the shard groups before stopping the legacy writer"
kn apply -f "$ROOT/deploy/k8s/networkpolicy.yaml"
sed "s#image: argus/canonical-shard#image: ${IMAGE_PREFIX}argus/canonical-shard:$IMAGE_TAG#" \
  "$ROOT/overlays/sharded/postgresql.yaml" | kn apply -f -
for shard in shard-0 shard-1 shard-2; do kn rollout status "statefulset/$shard" --timeout=300s; done

say "pause the Aggregator for a consistent copy"
trap 'kn scale deployment/aggregator --replicas="$replicas" >/dev/null' EXIT
kn scale deployment/aggregator --replicas=0
deadline=$((SECONDS+120))
until [ -z "$(kn get pods -l app=aggregator -o name)" ]; do
  [ "$SECONDS" -lt "$deadline" ] || die "legacy Aggregator did not drain"
  sleep 2
done

# A unique Job makes retries possible after an interrupted migration.
job="canonical-migration-$(date +%s)"
cat <<YAML | kn create -f -
apiVersion: batch/v1
kind: Job
metadata:
  name: $job
spec:
  backoffLimit: 0
  ttlSecondsAfterFinished: 900
  template:
    metadata:
      labels: {app: canonical-migration, role: other, bnpb.io/canonical-client: "true"}
    spec:
      restartPolicy: Never
      containers:
        - name: migration
          image: ${IMAGE_PREFIX}argus/aggregator:$IMAGE_TAG
          imagePullPolicy: IfNotPresent
          args: [migrate-single]
          envFrom:
            - secretRef: {name: aggregator-credentials}
          resources:
            requests: {memory: 128Mi}
            limits: {memory: 256Mi}
YAML
if ! kn wait --for=condition=complete "job/$job" --timeout=600s; then
  kn logs "job/$job" || true
  die "canonical migration did not finish"
fi
kn logs "job/$job"
# On success up.sh applies the new Aggregator configuration and replicas.
trap - EXIT
