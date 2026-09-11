# SPDX-License-Identifier: LGPL-3.0-or-later
# Added for TC35 integration (2026-08-01).

"""Kafka configuration for the TC35 NFStream meter workers.

This module keeps the TC35 performance settings for ``nfmod`` while
serializing values with the repository's strict JSON contract.
"""

from __future__ import annotations

import os
from typing import Any

from kafka import KafkaProducer

from tc35_contract import json_dumps_bytes


DEFAULT_BATCH_SIZE = 32_768
DEFAULT_LINGER_MS = 10
DEFAULT_COMPRESSION_TYPE = "gzip"
DEFAULT_FLUSH_PACKET_INTERVAL = 900
DEFAULT_CLEANUP_FLUSH_FLOW_INTERVAL = 300


def _integer_setting(name: str, default: int, *, minimum: int) -> int:
    raw_value = os.getenv(name, str(default))
    try:
        value = int(raw_value)
    except ValueError as exc:
        raise ValueError(f"{name} must be an integer") from exc
    if value < minimum:
        raise ValueError(f"{name} must be at least {minimum}")
    return value


def flush_packet_interval() -> int:
    return _integer_setting(
        "KAFKA_FLUSH_PACKET_INTERVAL",
        DEFAULT_FLUSH_PACKET_INTERVAL,
        minimum=1,
    )


def cleanup_flush_flow_interval() -> int:
    return _integer_setting(
        "KAFKA_CLEANUP_FLUSH_FLOW_INTERVAL",
        DEFAULT_CLEANUP_FLUSH_FLOW_INTERVAL,
        minimum=1,
    )


def _acks_setting() -> int | str:
    value = os.getenv("KAFKA_ACKS", "1").strip().lower()
    if value in {"0", "1", "-1"}:
        return int(value)
    if value == "all":
        return value
    raise ValueError("KAFKA_ACKS must be 0, 1, -1, or all")


def build_meter_producer(kafka_url: str, root_idx: int) -> KafkaProducer:
    """Create the producer inside one nfmod meter process."""
    client_id = os.getenv("PRODUCER_CLIENT_ID", "nfstream-producer")
    compression_type = os.getenv(
        "KAFKA_COMPRESSION_TYPE",
        DEFAULT_COMPRESSION_TYPE,
    ).strip()
    options: dict[str, Any] = {
        "bootstrap_servers": kafka_url,
        "client_id": f"{client_id}-meter-{root_idx}-{os.getpid()}",
        "key_serializer": lambda value: value.encode("utf-8"),
        "value_serializer": json_dumps_bytes,
        "batch_size": _integer_setting(
            "KAFKA_BATCH_SIZE",
            DEFAULT_BATCH_SIZE,
            minimum=1,
        ),
        "linger_ms": _integer_setting(
            "KAFKA_LINGER_MS",
            DEFAULT_LINGER_MS,
            minimum=0,
        ),
        "acks": _acks_setting(),
        "retries": _integer_setting("KAFKA_RETRIES", 0, minimum=0),
    }
    if compression_type:
        options["compression_type"] = compression_type
    return KafkaProducer(**options)
