#!/bin/sh
set -eu

partitions="${KAFKA_TOPIC_PARTITIONS:-8}"
case "$partitions" in
  ''|*[!0-9]*)
    echo "KAFKA_TOPIC_PARTITIONS must be a positive integer" >&2
    exit 2
    ;;
esac
if [ "$partitions" -lt 1 ]; then
  echo "KAFKA_TOPIC_PARTITIONS must be at least one" >&2
  exit 2
fi

for topic in inference_data inference_probs predicted_labels; do
  /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server kafka:9092 \
    --create \
    --if-not-exists \
    --topic "$topic" \
    --partitions "$partitions" \
    --replication-factor 1

  current_partitions="$(
    /opt/kafka/bin/kafka-topics.sh \
      --bootstrap-server kafka:9092 \
      --describe \
      --topic "$topic" |
      awk 'NR == 1 {
        for (field = 1; field <= NF; field++) {
          if ($field == "PartitionCount:") {
            print $(field + 1)
            exit
          }
        }
      }'
  )"
  if [ -z "$current_partitions" ]; then
    echo "Could not determine partition count for $topic" >&2
    exit 1
  fi
  if [ "$current_partitions" -lt "$partitions" ]; then
    /opt/kafka/bin/kafka-topics.sh \
      --bootstrap-server kafka:9092 \
      --alter \
      --topic "$topic" \
      --partitions "$partitions"
  elif [ "$current_partitions" -gt "$partitions" ]; then
    echo "$topic already has $current_partitions partitions; Kafka cannot shrink it" >&2
  fi
done

/opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --list
