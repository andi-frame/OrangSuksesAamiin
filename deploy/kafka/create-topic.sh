#!/bin/sh
# One-shot job (service kafka-init): create the topics explicitly, so that
# topic auto-creation can stay disabled on the broker. Idempotent.
#
#   hazard.events            Aggregator outbox -> consumers (M1 P5)
#   field-reports            Client-Facing API -> field-report processor (M2 P3 criterion 6)
#   field-reports.processed  processor output, compacted by report_id (its dedup set is rebuilt from it)
#
# KAFKA_EXTRA_TOPICS adds topics: "name:partitions:config,config name:..." (space separated).
set -eu
BOOTSTRAP="${KAFKA_BOOTSTRAP:-kafka:9092}"
TOPIC="${KAFKA_TOPIC:-hazard.events}"
PARTITIONS="${KAFKA_PARTITIONS:-3}"
RETENTION_MS="${KAFKA_RETENTION_MS:-604800000}"
EXTRA="${KAFKA_EXTRA_TOPICS:-field-reports:3:retention.ms=604800000 field-reports.processed:3:cleanup.policy=compact}"

create() { # name partitions config[,config...]
  set -- "$1" "$2" "$3"
  cfg=""
  oldifs=$IFS; IFS=,
  for c in $3; do cfg="$cfg --config $c"; done
  IFS=$oldifs
  # shellcheck disable=SC2086
  /opt/kafka/bin/kafka-topics.sh --bootstrap-server "$BOOTSTRAP" --create --if-not-exists \
    --topic "$1" --partitions "$2" --replication-factor 1 $cfg
  /opt/kafka/bin/kafka-topics.sh --bootstrap-server "$BOOTSTRAP" --describe --topic "$1"
  echo "topic $1 ready"
}

create "$TOPIC" "$PARTITIONS" "retention.ms=$RETENTION_MS"
for t in $EXTRA; do
  name=$(echo "$t" | cut -d: -f1)
  parts=$(echo "$t" | cut -d: -f2)
  cfg=$(echo "$t" | cut -d: -f3-)
  create "$name" "$parts" "$cfg"
done
