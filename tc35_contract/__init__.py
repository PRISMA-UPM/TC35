"""Shared message contract for the TC35 streaming pipeline."""

from .messages import (
    ContractError,
    FEATURE_NAMES_V1,
    SCHEMA_VERSION,
    SNAPSHOT_INTERVAL_SECONDS_V1,
    build_snapshot,
    decode_headers,
    extract_snapshot_batch,
    iter_polled_records,
    json_dumps_bytes,
    json_loads_bytes,
    normalize_snapshot_batch,
    ordered_feature_row,
)

__all__ = [
    "ContractError",
    "FEATURE_NAMES_V1",
    "SCHEMA_VERSION",
    "SNAPSHOT_INTERVAL_SECONDS_V1",
    "build_snapshot",
    "decode_headers",
    "extract_snapshot_batch",
    "iter_polled_records",
    "json_dumps_bytes",
    "json_loads_bytes",
    "normalize_snapshot_batch",
    "ordered_feature_row",
]
