"""Versioned JSON contract shared by NFStream, inference, and detection."""

from __future__ import annotations

import json
import math
from collections.abc import Mapping, Sequence
from numbers import Real
from typing import Any

SCHEMA_VERSION = "v1"
SNAPSHOT_INTERVAL_SECONDS_V1 = 0.5

# The order is part of the rev4 model contract.
FEATURE_NAMES_V1 = (
    "udps.protocol",
    "udps.src2dst_last",
    "udps.dst2src_last",
    "udps.src2dst_pkts_data",
    "udps.dst2src_pkts_data",
    "src2dst_bytes",
    "dst2src_bytes",
    "udps.diff_dst_src_first",
)


class ContractError(ValueError):
    """Raised when a Kafka payload does not match the supported schema."""


def _json_default(value: Any) -> Any:
    """Convert NumPy-like scalar values without importing NumPy."""
    item = getattr(value, "item", None)
    if callable(item):
        return item()
    raise TypeError(f"{type(value).__name__} is not JSON serializable")


def json_dumps_bytes(value: Any) -> bytes:
    """Serialize a Kafka value as strict, compact UTF-8 JSON."""
    return json.dumps(
        value,
        allow_nan=False,
        default=_json_default,
        ensure_ascii=False,
        separators=(",", ":"),
    ).encode("utf-8")


def json_loads_bytes(value: bytes) -> Any:
    """Deserialize strict UTF-8 JSON from a Kafka value."""
    if not isinstance(value, (bytes, bytearray)):
        raise TypeError("Kafka value must be bytes")

    def reject_nonfinite(constant: str) -> None:
        raise ValueError(f"non-finite JSON number is not allowed: {constant}")

    return json.loads(
        bytes(value).decode("utf-8"),
        parse_constant=reject_nonfinite,
    )


def decode_headers(headers: Sequence[tuple[Any, bytes | None]] | None) -> dict[str, str]:
    """Decode Kafka headers into a last-value-wins dictionary."""
    decoded: dict[str, str] = {}
    for raw_key, raw_value in headers or ():
        key = raw_key.decode("utf-8") if isinstance(raw_key, bytes) else str(raw_key)
        decoded[key] = "" if raw_value is None else raw_value.decode("utf-8")
    return decoded


def iter_polled_records(polled: Mapping[Any, Sequence[Any]]):
    """Yield every record returned by KafkaConsumer.poll(), across partitions."""
    for records in polled.values():
        yield from records


def _parse_feature_names(value: Any) -> tuple[str, ...]:
    if isinstance(value, str):
        names = tuple(part.strip() for part in value.split(",") if part.strip())
    elif isinstance(value, Sequence) and not isinstance(value, (bytes, bytearray)):
        names = tuple(str(part).strip() for part in value if str(part).strip())
    else:
        raise ContractError("feature_names must be a comma-separated string or a list")

    if not names or len(set(names)) != len(names):
        raise ContractError("feature_names must contain unique, non-empty names")
    return names


def normalize_snapshot_batch(value: Any) -> list[dict[str, Any]]:
    """Accept the current single-snapshot form and the legacy batched form."""
    if isinstance(value, Mapping):
        return [dict(value)]
    if isinstance(value, list) and value and all(isinstance(item, Mapping) for item in value):
        return [dict(item) for item in value]
    raise ContractError("Kafka value must be one snapshot object or a non-empty list of snapshots")


def ordered_feature_row(
    snapshot: Mapping[str, Any],
    expected_feature_names: Sequence[str] = FEATURE_NAMES_V1,
) -> dict[str, Real]:
    """Validate a snapshot and return features in model order."""
    data = snapshot.get("data")
    if not isinstance(data, Mapping):
        raise ContractError("snapshot.data must be an object")
    features = data.get("features")
    if not isinstance(features, Mapping):
        raise ContractError("snapshot.data.features must be an object")

    expected = tuple(expected_feature_names)
    actual = set(features)
    missing = [name for name in expected if name not in actual]
    extra = sorted(actual.difference(expected))
    if missing or extra:
        raise ContractError(f"feature schema mismatch; missing={missing}, extra={extra}")

    row: dict[str, Real] = {}
    for name in expected:
        value = features[name]
        if isinstance(value, bool) or not isinstance(value, Real):
            raise ContractError(f"feature {name!r} must be numeric")
        if not math.isfinite(float(value)):
            raise ContractError(f"feature {name!r} must be finite")
        row[name] = value
    return row


def extract_snapshot_batch(
    value: Any,
    headers: Sequence[tuple[Any, bytes | None]] | None,
    expected_feature_names: Sequence[str] = FEATURE_NAMES_V1,
    expected_interval_seconds: float = SNAPSHOT_INTERVAL_SECONDS_V1,
) -> tuple[list[dict[str, Real]], list[dict[str, Any]], str]:
    """Normalize and validate a Kafka record for model inference."""
    decoded_headers = decode_headers(headers)
    expected = tuple(expected_feature_names)
    rows: list[dict[str, Real]] = []
    metadata: list[dict[str, Any]] = []
    versions: set[str] = set()

    for snapshot in normalize_snapshot_batch(value):
        version = str(snapshot.get("schema_version") or decoded_headers.get("version") or "")
        if version != SCHEMA_VERSION:
            raise ContractError(f"unsupported schema version {version!r}")
        versions.add(version)

        declared_raw = snapshot.get("feature_names") or decoded_headers.get("features_names")
        if declared_raw is None:
            raise ContractError("feature_names are required in the payload or Kafka headers")
        declared = _parse_feature_names(declared_raw)
        if declared != expected:
            raise ContractError(
                f"declared feature order does not match model schema: {declared!r}"
            )

        raw_interval = snapshot.get("snapshot_interval_seconds")
        if raw_interval is None:
            raw_metadata = snapshot.get("metadata")
            if isinstance(raw_metadata, Mapping):
                interval_start = raw_metadata.get("timestamp_ini_interval")
                interval_end = raw_metadata.get("timestamp_fin_interval")
                if (
                    isinstance(interval_start, Real)
                    and not isinstance(interval_start, bool)
                    and isinstance(interval_end, Real)
                    and not isinstance(interval_end, bool)
                ):
                    raw_interval = (float(interval_end) - float(interval_start)) / 1000.0
        if (
            isinstance(raw_interval, bool)
            or not isinstance(raw_interval, Real)
            or not math.isfinite(float(raw_interval))
        ):
            raise ContractError("snapshot_interval_seconds must be a finite number")
        if not math.isclose(
            float(raw_interval),
            expected_interval_seconds,
            rel_tol=0.0,
            abs_tol=1e-9,
        ):
            raise ContractError(
                "snapshot interval does not match model contract: "
                f"{raw_interval!r} != {expected_interval_seconds!r}"
            )

        rows.append(ordered_feature_row(snapshot, expected))

        raw_metadata = snapshot.get("metadata")
        if not isinstance(raw_metadata, Mapping):
            raise ContractError("snapshot.metadata must be an object")
        connection = raw_metadata.get("connection_id", {})
        if not isinstance(connection, Mapping):
            raise ContractError("snapshot.metadata.connection_id must be an object")
        flattened = dict(connection)
        flattened.update(
            (key, item)
            for key, item in raw_metadata.items()
            if key != "connection_id"
        )
        metadata.append(flattened)

    if len(versions) != 1:
        raise ContractError("all snapshots in one Kafka record must use the same schema version")
    return rows, metadata, versions.pop()


def build_snapshot(
    features: Mapping[str, Real],
    metadata: Mapping[str, Any],
    feature_names: Sequence[str] = FEATURE_NAMES_V1,
    interval_seconds: float = SNAPSHOT_INTERVAL_SECONDS_V1,
) -> dict[str, Any]:
    """Build and validate a versioned NFStream snapshot."""
    if (
        isinstance(interval_seconds, bool)
        or not isinstance(interval_seconds, Real)
        or not math.isfinite(float(interval_seconds))
        or not math.isclose(
            float(interval_seconds),
            SNAPSHOT_INTERVAL_SECONDS_V1,
            rel_tol=0.0,
            abs_tol=1e-9,
        )
    ):
        raise ContractError(
            "NFStream v1 requires snapshot_interval_seconds="
            f"{SNAPSHOT_INTERVAL_SECONDS_V1}"
        )
    snapshot = {
        "schema_version": SCHEMA_VERSION,
        "snapshot_interval_seconds": interval_seconds,
        "feature_names": list(feature_names),
        "data": {"features": dict(features)},
        "metadata": dict(metadata),
    }
    ordered_feature_row(snapshot, feature_names)
    return snapshot
