#!/usr/bin/env bash
# Switch Sensor Pantai between the practice and the exam profile (demo day), image unchanged.
#   bash scripts/k8s/sensor-exam.sh exam <key-file>   Secret pantai-key (file "key", from the assistants), PANTAI_PROFILE=exam
#   bash scripts/k8s/sensor-exam.sh practice          back to the practice profile
# The sensor restarts from scratch (emptyDir); the Aggregator notices (generated_at goes backwards) and re-reads
# from the start - storing is idempotent on observation_id.
. "$(dirname "${BASH_SOURCE[0]}")/common.sh"
case "${1:-}" in
  exam)
    key="${2:-}"
    [ -f "$key" ] || die "usage: $0 exam <key-file>"
    kn create secret generic pantai-key --from-file=key="$key" --dry-run=client -o yaml | kn apply -f - >/dev/null
    kn patch deploy/sensor-pantai --type=strategic -p '{"spec":{"template":{"spec":{
      "volumes":[{"name":"key","secret":{"secretName":"pantai-key"}}],
      "containers":[{"name":"sensor","env":[{"name":"PANTAI_PROFILE","value":"exam"}],
        "volumeMounts":[{"name":"key","mountPath":"/var/run/pantai","readOnly":true}]}]}}}}'
    ;;
  practice)
    kn patch deploy/sensor-pantai --type=strategic -p '{"spec":{"template":{"spec":{
      "containers":[{"name":"sensor","env":[{"name":"PANTAI_PROFILE","value":"practice"}]}]}}}}'
    ;;
  *) die "usage: $0 exam <key-file> | practice" ;;
esac
kn rollout status deploy/sensor-pantai --timeout=120s
say "sensor profile: $(kn get deploy sensor-pantai -o jsonpath='{.spec.template.spec.containers[0].env[?(@.name=="PANTAI_PROFILE")].value}')"
kn logs deploy/sensor-pantai --tail=5
