#!/usr/bin/env bash
# Bonus: autoscaling of client-api and aggregator (deploy/hpa/hpa.yaml).  on | off | status
. "$(dirname "${BASH_SOURCE[0]}")/common.sh"
case "${1:-status}" in
  on)
    kc get apiservice v1beta1.metrics.k8s.io >/dev/null 2>&1 || die "metrics-server missing (k3s ships it; kind: install metrics-server)"
    kn apply -f "$ROOT/deploy/hpa/hpa.yaml" ;;
  off) kn delete -f "$ROOT/deploy/hpa/hpa.yaml" --ignore-not-found ;;
  status) kn get hpa 2>/dev/null || true; kn top pods -l 'app in (aggregator,client-api)' 2>/dev/null || true ;;
  *) die "usage: $0 on | off | status" ;;
esac
