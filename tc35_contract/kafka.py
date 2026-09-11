"""Kafka delivery helpers used by TC35 consumers."""

from __future__ import annotations

from typing import Any

from kafka.structs import OffsetAndMetadata, TopicPartition


def commit_processed_record(consumer: Any, record: Any) -> None:
    """Commit exactly the record after a durable side effect or deliberate skip."""
    offsets = {
        TopicPartition(record.topic, record.partition): OffsetAndMetadata(
            record.offset + 1,
            "",
            -1,
        )
    }
    consumer.commit(offsets=offsets)
