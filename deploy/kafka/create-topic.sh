#!/bin/sh
# One-shot job (service kafka-init): create the hazard topic explicitly, so that
# topic auto-creation can stay disabled on the broker. Idempotent.
set -eu
BOOTSTRAP="${KAFKA_BOOTSTRAP:-kafka:9092}"
TOPIC="${KAFKA_TOPIC:-hazard.events}"
PARTITIONS="${KAFKA_PARTITIONS:-3}"
RETENTION_MS="${KAFKA_RETENTION_MS:-604800000}"

/opt/kafka/bin/kafka-topics.sh --bootstrap-server "$BOOTSTRAP" --create --if-not-exists \
  --topic "$TOPIC" --partitions "$PARTITIONS" --replication-factor 1 \
  --config "retention.ms=$RETENTION_MS"
/opt/kafka/bin/kafka-topics.sh --bootstrap-server "$BOOTSTRAP" --describe --topic "$TOPIC"
echo "topic $TOPIC ready"
