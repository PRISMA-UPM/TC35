"""Exercise the running TC35 inference and detector services through Kafka."""

from __future__ import annotations

import os
import time
import uuid

from kafka import KafkaConsumer, KafkaProducer, TopicPartition

from tc35_contract import (
    FEATURE_NAMES_V1,
    build_snapshot,
    iter_polled_records,
    json_dumps_bytes,
    json_loads_bytes,
)


def main() -> None:
    kafka_url = os.getenv(
        "SMOKE_KAFKA_URL",
        os.getenv("KAFKA_URL", "127.0.0.1:9094"),
    )
    input_topic = os.getenv("SMOKE_INPUT_TOPIC", "inference_data")
    output_topic = os.getenv("SMOKE_OUTPUT_TOPIC", "predicted_labels")
    smoke_id = str(uuid.uuid4())

    consumer = KafkaConsumer(
        bootstrap_servers=kafka_url,
        client_id=f"tc35-smoke-consumer-{smoke_id}",
        group_id=None,
        value_deserializer=json_loads_bytes,
        enable_auto_commit=False,
    )
    # Pin this observer to the current end offsets before publishing. Explicit
    # assignment avoids group/subscription races that can skip a fast response.
    metadata_deadline = time.monotonic() + 10
    partitions = consumer.partitions_for_topic(output_topic)
    while not partitions and time.monotonic() < metadata_deadline:
        time.sleep(0.1)
        partitions = consumer.partitions_for_topic(output_topic)
    if not partitions:
        raise TimeoutError("predicted_labels topic metadata timed out")
    topic_partitions = [
        TopicPartition(output_topic, partition)
        for partition in sorted(partitions)
    ]
    consumer.assign(topic_partitions)
    end_offsets = consumer.end_offsets(topic_partitions)
    for topic_partition in topic_partitions:
        consumer.seek(topic_partition, end_offsets[topic_partition])

    producer = KafkaProducer(
        bootstrap_servers=kafka_url,
        client_id=f"tc35-smoke-producer-{smoke_id}",
        value_serializer=json_dumps_bytes,
        acks="all",
    )
    try:
        features = {
            "udps.protocol": 6,
            "udps.src2dst_last": 0.45,
            "udps.dst2src_last": 0.42,
            "udps.src2dst_pkts_data": 8,
            "udps.dst2src_pkts_data": 6,
            "src2dst_bytes": 1_024,
            "dst2src_bytes": 768,
            "udps.diff_dst_src_first": 0.01,
        }
        snapshot = build_snapshot(
            features,
            {
                "connection_id": {
                    "src_ip": "192.0.2.250",
                    "src_port": 45_000,
                    "dst_ip": "198.51.100.250",
                    "dst_port": 443,
                    "protocol": 6,
                },
                "timestamp": int(time.time() * 1_000),
                "timestamp_nfstream": time.time(),
                "monitored_device": "pipeline-smoke-test",
                "interface": "synthetic",
                "flow_bytes": 1_792,
                "flow_pkts": 14,
                "smoke_id": smoke_id,
            },
            FEATURE_NAMES_V1,
        )
        producer.send(input_topic, value=snapshot).get(timeout=30)

        prediction_deadline = time.monotonic() + 30
        while time.monotonic() < prediction_deadline:
            remaining_ms = max(
                1,
                min(1_000, int((prediction_deadline - time.monotonic()) * 1000)),
            )
            for record in iter_polled_records(
                consumer.poll(timeout_ms=remaining_ms)
            ):
                payload = record.value
                metadata = payload.get("metadata", [])
                if not metadata or metadata[0].get("smoke_id") != smoke_id:
                    continue
                if payload.get("schema_version") != "v1":
                    raise RuntimeError(f"unexpected schema: {payload!r}")
                if len(payload.get("data", [])) != 1:
                    raise RuntimeError(f"unexpected prediction count: {payload!r}")
                confidence = metadata[0].get("ml_confidence")
                if (
                    not isinstance(confidence, (int, float))
                    or not 0 <= confidence <= 1
                ):
                    raise RuntimeError(f"invalid confidence: {payload!r}")
                print(
                    "TC35 pipeline smoke test passed: "
                    f"label={payload['data'][0]!r}, confidence={confidence:.6f}, "
                    f"smoke_id={smoke_id}"
                )
                return
        raise TimeoutError("no matching prediction reached predicted_labels in 30 seconds")
    finally:
        producer.flush(timeout=30)
        producer.close(timeout=30)
        consumer.close()


if __name__ == "__main__":
    main()
