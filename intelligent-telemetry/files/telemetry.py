"""Aggregate classified TC35 events and write experiment telemetry to InfluxDB."""

from __future__ import annotations

import argparse
import csv
import logging
import math
import os
import signal
import time
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, TextIO

from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
from kafka import KafkaConsumer

from tc35_contract import SCHEMA_VERSION, iter_polled_records, json_loads_bytes
from tc35_contract.kafka import commit_processed_record

LOGGER = logging.getLogger("tc35.telemetry")


class TelemetryError(ValueError):
    """Raised when detector output cannot be monitored safely."""


def _required_number(
    item: Mapping[str, Any],
    key: str,
    *,
    minimum: float | None = None,
    maximum: float | None = None,
) -> float:
    value = item.get(key)
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise TelemetryError(f"{key} must be numeric")
    result = float(value)
    if not math.isfinite(result):
        raise TelemetryError(f"{key} must be finite")
    if minimum is not None and result < minimum:
        raise TelemetryError(f"{key} must be at least {minimum}")
    if maximum is not None and result > maximum:
        raise TelemetryError(f"{key} must be at most {maximum}")
    return result


def _optional_number(
    item: Mapping[str, Any],
    key: str,
    *,
    default: float,
    minimum: float | None = None,
) -> float:
    if key not in item:
        return default
    return _required_number(item, key, minimum=minimum)


def _connection_value(item: Mapping[str, Any], key: str) -> Any:
    """Read both the current nested connection_id and legacy flattened metadata."""
    if key in item:
        return item[key]
    connection = item.get("connection_id")
    if isinstance(connection, Mapping) and key in connection:
        return connection[key]
    return "unknown"


def _connection_key(item: Mapping[str, Any]) -> tuple[str, ...]:
    """Identify a cumulative NFStream flow without merging reused connections."""
    return tuple(
        str(_connection_value(item, key))
        for key in (
            "src_ip",
            "dst_ip",
            "src_port",
            "dst_port",
            "protocol",
            "first",
        )
    )


def _unique_connection_key(item: Mapping[str, Any]) -> tuple[str, ...]:
    """Use the four-field identity required by the monitoring metrics."""
    return tuple(
        str(_connection_value(item, key))
        for key in ("src_ip", "dst_ip", "src_port", "dst_port")
    )


def validated_events(
    payload: Any,
    *,
    observed_at: float | None = None,
    require_event_timestamp: bool = True,
) -> list[dict[str, Any]]:
    """Validate detector output and expose normalized events for both sinks."""
    if not isinstance(payload, Mapping):
        raise TelemetryError("detector payload must be an object")
    if payload.get("schema_version") != SCHEMA_VERSION:
        raise TelemetryError(
            f"unsupported schema version {payload.get('schema_version')!r}"
        )
    labels = payload.get("data")
    metadata = payload.get("metadata")
    if not isinstance(labels, list) or not isinstance(metadata, list):
        raise TelemetryError("data and metadata must be lists")
    if len(labels) != len(metadata):
        raise TelemetryError("metadata length must equal label count")

    monitoring_at = time.time() if observed_at is None else float(observed_at)
    if not math.isfinite(monitoring_at) or monitoring_at < 0:
        raise TelemetryError("observed_at must be a finite non-negative timestamp")

    events: list[dict[str, Any]] = []
    for label, item in zip(labels, metadata, strict=True):
        if not isinstance(label, str) or not label:
            raise TelemetryError("each label must be a non-empty string")
        if not isinstance(item, Mapping):
            raise TelemetryError("each metadata entry must be an object")
        if item.get("label") not in (None, label):
            raise TelemetryError("metadata label does not match data label")

        detected_at = _required_number(item, "timestamp_detector", minimum=0.0)
        inference_at = _required_number(item, "timestamp_inference", minimum=0.0)
        nfstream_at = _required_number(item, "timestamp_nfstream", minimum=0.0)
        if require_event_timestamp:
            event_at = _required_number(item, "timestamp", minimum=0.0)
        else:
            event_at = _optional_number(
                item,
                "timestamp",
                default=nfstream_at,
                minimum=0.0,
            )
        # NFStream packet timestamps are milliseconds before inference normalizes them.
        if event_at > 10_000_000_000:
            event_at /= 1000.0

        confidence = _required_number(
            item,
            "ml_confidence",
            minimum=0.0,
            maximum=1.0,
        )
        flow_bytes = _required_number(item, "flow_bytes", minimum=0.0)
        flow_pkts = _required_number(item, "flow_pkts", minimum=0.0)
        tags = {
            "label": label,
            "monitored_device": str(item.get("monitored_device", "unknown")),
            "interface": str(item.get("interface", "unknown")),
        }
        events.append(
            {
                "label": label,
                "event_at": event_at,
                "nfstream_at": nfstream_at,
                "inference_at": inference_at,
                "detected_at": detected_at,
                "monitoring_at": monitoring_at,
                "confidence": confidence,
                "flow_bytes": flow_bytes,
                "flow_pkts": flow_pkts,
                "connection": _connection_key(item),
                "unique_connection": _unique_connection_key(item),
                "src_ip": str(_connection_value(item, "src_ip")),
                "dst_ip": str(_connection_value(item, "dst_ip")),
                "tags": tags,
            }
        )
    return events


def _prediction_record(
    event: Mapping[str, Any],
    *,
    sequence: int = 0,
) -> dict[str, Any]:
    return {
        "measurement": "prediction",
        # A detector batch shares one timestamp. The stable sequence prevents
        # same-label points in that batch from colliding in InfluxDB.
        "timestamp_ns": int(float(event["detected_at"]) * 1_000_000_000) + sequence,
        "tags": dict(event["tags"]),
        "fields": {
            "count": 1,
            "ml_confidence": float(event["confidence"]),
            "flow_bytes": float(event["flow_bytes"]),
            "flow_pkts": float(event["flow_pkts"]),
            "src_ip": str(event["src_ip"]),
            "dst_ip": str(event["dst_ip"]),
            "inference_latency_ms": max(
                0.0,
                (float(event["inference_at"]) - float(event["nfstream_at"]))
                * 1000.0,
            ),
            "detection_latency_ms": max(
                0.0,
                (float(event["detected_at"]) - float(event["inference_at"]))
                * 1000.0,
            ),
            "monitoring_latency_ms": max(
                0.0,
                (float(event["monitoring_at"]) - float(event["detected_at"]))
                * 1000.0,
            ),
            "total_pipeline_latency_ms": max(
                0.0,
                (float(event["monitoring_at"]) - float(event["nfstream_at"]))
                * 1000.0,
            ),
        },
    }


def measurement_records(
    payload: Any,
    *,
    observed_at: float | None = None,
) -> list[dict[str, Any]]:
    """Turn detector output into the existing per-snapshot measurements."""
    return [
        _prediction_record(event, sequence=index)
        for index, event in enumerate(
            validated_events(
                payload,
                observed_at=observed_at,
                require_event_timestamp=False,
            )
        )
    ]


@dataclass
class _ClassMetrics:
    bytes: float = 0.0
    packets: float = 0.0
    snapshots: int = 0
    confidence_by_connection: dict[tuple[str, ...], tuple[float, int]] = field(
        default_factory=dict
    )

    def add_confidence(self, connection: tuple[str, ...], confidence: float) -> None:
        total, count = self.confidence_by_connection.get(connection, (0.0, 0))
        self.confidence_by_connection[connection] = (total + confidence, count + 1)

    def mean_connection_confidence(self) -> float:
        """Average each connection first, then average across connections."""
        means = [
            total / count
            for total, count in self.confidence_by_connection.values()
            if count
        ]
        return sum(means) / len(means) if means else 0.0


class FlowDeltaTracker:
    """Convert NFStream cumulative flow counters into per-snapshot increments."""

    def __init__(self) -> None:
        self._counters: dict[tuple[str, ...], tuple[float, float]] = {}

    def observe(
        self,
        connection: tuple[str, ...],
        flow_bytes: float,
        flow_pkts: float,
    ) -> tuple[float, float]:
        previous = self._counters.get(connection)
        if previous is None:
            self._counters[connection] = (flow_bytes, flow_pkts)
            return flow_bytes, flow_pkts

        previous_bytes, previous_pkts = previous
        # Duplicated or out-of-order cumulative snapshots must not double count.
        if flow_pkts <= previous_pkts:
            return 0.0, 0.0
        self._counters[connection] = (flow_bytes, flow_pkts)
        return max(0.0, flow_bytes - previous_bytes), flow_pkts - previous_pkts


@dataclass
class _WindowMetrics:
    start: float
    end: float
    monitored_device: str = "unknown"
    interface: str = "unknown"
    snapshots: int = 0
    accepted: int = 0
    late: int = 0
    rejected: int = 0
    malign: int = 0
    by_class: dict[str, _ClassMetrics] = field(default_factory=dict)
    hh_connections: set[tuple[str, ...]] = field(default_factory=set)
    hh_sources: set[str] = field(default_factory=set)
    hh_targets: set[str] = field(default_factory=set)
    attack_connections: set[tuple[str, ...]] = field(default_factory=set)
    ddos_attackers: set[str] = field(default_factory=set)
    ddos_targets: set[str] = field(default_factory=set)


class EventWindowAggregator:
    """Aggregate snapshots in event-time windows with one-window lateness."""

    def __init__(
        self,
        *,
        window_seconds: float = 5.0,
        allowed_lateness_seconds: float | None = None,
        benign_hh_labels: Sequence[str] = ("benign_heavy_hitter",),
        malign_labels: Sequence[str] = ("malign_heavy_hitter",),
        monitored_labels: Sequence[str] = (
            "normal_traffic",
            "benign_heavy_hitter",
            "malign_heavy_hitter",
        ),
        rebase_event_time: bool = True,
        idle_flush_seconds: float = 1.0,
    ) -> None:
        if not math.isfinite(window_seconds) or window_seconds <= 0:
            raise ValueError("window_seconds must be positive and finite")
        lateness = window_seconds if allowed_lateness_seconds is None else allowed_lateness_seconds
        if not math.isfinite(lateness) or lateness < 0:
            raise ValueError("allowed_lateness_seconds must be finite and non-negative")
        if not math.isfinite(idle_flush_seconds) or idle_flush_seconds < 0:
            raise ValueError("idle_flush_seconds must be finite and non-negative")

        self.window_seconds = float(window_seconds)
        self.allowed_lateness_seconds = float(lateness)
        self.benign_hh_labels = frozenset(benign_hh_labels)
        self.malign_labels = frozenset(malign_labels)
        self.monitored_labels = frozenset(monitored_labels)
        self.rebase_event_time = rebase_event_time
        self.idle_flush_seconds = float(idle_flush_seconds)
        self.flow_deltas = FlowDeltaTracker()
        self._window: _WindowMetrics | None = None
        self._event_anchor: float | None = None
        self._plot_anchor: float | None = None
        self._last_observed_at: float | None = None
        self._dirty = False
        self._max_event_at: float | None = None
        self._future_events: list[tuple[dict[str, Any], float]] = []

    def _ensure_window(self, event_at: float, observed_at: float) -> None:
        if self._event_anchor is None:
            self._event_anchor = event_at
            self._plot_anchor = observed_at
        if self._window is None:
            self._window = _WindowMetrics(event_at, event_at + self.window_seconds)

    def _plot_time(self, event_time: float) -> float:
        if not self.rebase_event_time:
            return event_time
        assert self._event_anchor is not None and self._plot_anchor is not None
        return self._plot_anchor + (event_time - self._event_anchor)

    def _advance(self) -> None:
        assert self._window is not None
        previous = self._window
        start = self._window.end
        self._window = _WindowMetrics(
            start,
            start + self.window_seconds,
            monitored_device=previous.monitored_device,
            interface=previous.interface,
        )
        self._dirty = False

    def _target_window(self, event_at: float) -> tuple[float, float]:
        assert self._event_anchor is not None
        index = math.floor((event_at - self._event_anchor) / self.window_seconds)
        index = max(0, index)
        start = self._event_anchor + index * self.window_seconds
        return start, start + self.window_seconds

    def _apply_to_current(
        self,
        event: Mapping[str, Any],
    ) -> dict[str, Any]:
        assert self._window is not None
        event_at = float(event["event_at"])
        metrics = self._window
        metrics.snapshots += 1
        metrics.monitored_device = str(event["tags"]["monitored_device"])
        metrics.interface = str(event["tags"]["interface"])
        self._dirty = True

        too_late = event_at < metrics.start - self.allowed_lateness_seconds
        late = event_at < metrics.start
        disposition = {
            "accepted": not too_late,
            "late": late,
            "rejected": too_late,
            "window_start": metrics.start,
            "window_end": metrics.end,
            "plot_window_end": self._plot_time(metrics.end),
        }
        if late:
            metrics.late += 1
        if too_late:
            metrics.rejected += 1
            return disposition

        metrics.accepted += 1
        label = str(event["label"])
        class_metrics = metrics.by_class.setdefault(label, _ClassMetrics())
        delta_bytes, delta_packets = self.flow_deltas.observe(
            tuple(event["connection"]),
            float(event["flow_bytes"]),
            float(event["flow_pkts"]),
        )
        class_metrics.bytes += delta_bytes
        class_metrics.packets += delta_packets
        class_metrics.snapshots += 1
        class_metrics.add_confidence(
            tuple(event["unique_connection"]),
            float(event["confidence"]),
        )

        connection = tuple(event["unique_connection"])
        if label in self.benign_hh_labels:
            metrics.hh_connections.add(connection)
            metrics.hh_sources.add(str(event["src_ip"]))
            metrics.hh_targets.add(str(event["dst_ip"]))
        if label in self.malign_labels:
            metrics.malign += 1
            metrics.attack_connections.add(connection)
            metrics.ddos_attackers.add(str(event["src_ip"]))
            metrics.ddos_targets.add(str(event["dst_ip"]))
        return disposition

    def _drain_future_into_current(self) -> None:
        assert self._window is not None
        if not self._future_events:
            return
        self._future_events.sort(key=lambda pending: float(pending[0]["event_at"]))
        remaining: list[tuple[dict[str, Any], float]] = []
        for event, observed_at in self._future_events:
            if float(event["event_at"]) < self._window.end:
                self._apply_to_current(event)
                self._last_observed_at = max(
                    observed_at,
                    self._last_observed_at or observed_at,
                )
            else:
                remaining.append((event, observed_at))
        self._future_events = remaining

    def _drain_watermark(self) -> list[dict[str, Any]]:
        completed: list[dict[str, Any]] = []
        if self._window is None or self._max_event_at is None:
            return completed
        watermark = self._max_event_at - self.allowed_lateness_seconds
        while watermark >= self._window.end:
            completed.extend(self._finish_window())
            self._drain_future_into_current()
        return completed

    def _records(self, metrics: _WindowMetrics) -> list[dict[str, Any]]:
        timestamp_ns = int(self._plot_time(metrics.end) * 1_000_000_000)
        base_tags = {
            "monitored_device": metrics.monitored_device,
            "interface": metrics.interface,
        }
        records: list[dict[str, Any]] = []
        labels = self.monitored_labels.union(metrics.by_class)
        for label in sorted(labels):
            values = metrics.by_class.get(label, _ClassMetrics())
            records.append(
                {
                    "measurement": "traffic_window",
                    "timestamp_ns": timestamp_ns,
                    "tags": {**base_tags, "label": label},
                    "fields": {
                        "bytes": values.bytes,
                        "packets": values.packets,
                        "snapshots": values.snapshots,
                        "mean_confidence": values.mean_connection_confidence(),
                    },
                }
            )

        security_status = metrics.malign / metrics.accepted if metrics.accepted else 0.0
        records.append(
            {
                "measurement": "security_window",
                "timestamp_ns": timestamp_ns,
                "tags": base_tags,
                "fields": {
                    "security_status": security_status,
                    "snapshots": metrics.snapshots,
                    "accepted_snapshots": metrics.accepted,
                    "late_snapshots": metrics.late,
                    "rejected_snapshots": metrics.rejected,
                    "unique_attack_connections": len(metrics.attack_connections),
                    "unique_ddos_attackers": len(metrics.ddos_attackers),
                    "unique_ddos_targets": len(metrics.ddos_targets),
                    "unique_hh_connections": len(metrics.hh_connections),
                    "unique_hh_sources": len(metrics.hh_sources),
                    "unique_hh_targets": len(metrics.hh_targets),
                    "window_start": metrics.start,
                    "window_end": metrics.end,
                },
            }
        )
        return records

    def _finish_window(self) -> list[dict[str, Any]]:
        assert self._window is not None
        records = self._records(self._window)
        self._advance()
        return records

    def add_event(
        self,
        event: Mapping[str, Any],
        *,
        observed_at: float | None = None,
    ) -> tuple[list[dict[str, Any]], dict[str, Any]]:
        now = float(event["monitoring_at"] if observed_at is None else observed_at)
        event_at = float(event["event_at"])
        self._ensure_window(event_at, now)
        assert self._window is not None
        self._max_event_at = max(event_at, self._max_event_at or event_at)
        self._last_observed_at = now
        if event_at >= self._window.end:
            self._future_events.append((dict(event), now))
            target_start, target_end = self._target_window(event_at)
            disposition = {
                "accepted": True,
                "late": False,
                "rejected": False,
                "window_start": target_start,
                "window_end": target_end,
                "plot_window_end": self._plot_time(target_end),
            }
        else:
            disposition = self._apply_to_current(event)
        return self._drain_watermark(), disposition

    def flush_idle(self, *, observed_at: float | None = None) -> list[dict[str, Any]]:
        if self._window is None or self._last_observed_at is None:
            return []
        now = time.time() if observed_at is None else float(observed_at)
        if now - self._last_observed_at < self.idle_flush_seconds:
            return []
        completed: list[dict[str, Any]] = []
        # Rebased playback follows the wall clock and keeps producing the
        # zero-valued interval heartbeat used by the dashboard.
        while now >= self._plot_time(self._window.end):
            completed.extend(self._finish_window())
            self._drain_future_into_current()
            if not self.rebase_event_time and not self._dirty:
                break
        return completed

    def flush(self) -> list[dict[str, Any]]:
        if self._window is None:
            return []
        completed: list[dict[str, Any]] = []
        while self._future_events:
            self._drain_future_into_current()
            if self._future_events:
                completed.extend(self._finish_window())
        if self._dirty:
            completed.extend(self._finish_window())
        return completed


class CsvExports:
    """Optional durable experiment exports; disabled when paths are not supplied."""

    INTERVAL_HEADER = (
        "l_counter_late_per_interval",
        "l_counter_rejected_per_interval",
        "snapshots_per_interval",
        "accepted_per_interval",
        "window_start",
        "window_end",
    )
    LATENCY_HEADER = (
        "lt_nfstream_inference",
        "lt_inference_detector",
        "lt_detector_monitoring",
        "lt_total_delay",
        "event_timestamp",
        "late",
    )

    def __init__(
        self,
        *,
        interval_path: str | Path | None = None,
        latency_path: str | Path | None = None,
    ) -> None:
        self.interval_path = Path(interval_path) if interval_path else None
        self.latency_path = Path(latency_path) if latency_path else None
        self._interval_handle, self._interval_writer = self._open(
            self.interval_path,
            self.INTERVAL_HEADER,
        )
        self._latency_handle, self._latency_writer = self._open(
            self.latency_path,
            self.LATENCY_HEADER,
        )

    @staticmethod
    def _open(
        path: Path | None,
        header: Sequence[str],
    ) -> tuple[TextIO | None, Any | None]:
        if path is None:
            return None, None
        path.parent.mkdir(parents=True, exist_ok=True)
        needs_header = not path.exists() or path.stat().st_size == 0
        handle = path.open("a", newline="", encoding="utf-8")
        writer = csv.writer(handle)
        if needs_header:
            writer.writerow(header)
        return handle, writer

    def write_intervals(self, records: Sequence[Mapping[str, Any]]) -> None:
        for record in records:
            if record.get("measurement") != "security_window":
                continue
            fields = record["fields"]
            if self._interval_writer is None:
                continue
            self._interval_writer.writerow(
                (
                    fields["late_snapshots"],
                    fields["rejected_snapshots"],
                    fields["snapshots"],
                    fields["accepted_snapshots"],
                    fields["window_start"],
                    fields["window_end"],
                )
            )

    def write_latency(
        self,
        event: Mapping[str, Any],
        disposition: Mapping[str, Any],
    ) -> None:
        if not disposition["accepted"]:
            return
        if self._latency_writer is None:
            return
        self._latency_writer.writerow(
            (
                float(event["inference_at"]) - float(event["nfstream_at"]),
                float(event["detected_at"]) - float(event["inference_at"]),
                float(event["monitoring_at"]) - float(event["detected_at"]),
                float(event["monitoring_at"]) - float(disposition["plot_window_end"]),
                event["event_at"],
                int(bool(disposition["late"])),
            )
        )

    def flush(self) -> None:
        for handle in (self._interval_handle, self._latency_handle):
            if handle is not None:
                handle.flush()

    def close(self) -> None:
        for handle in (self._interval_handle, self._latency_handle):
            if handle is not None:
                handle.close()


def records_to_points(records: Sequence[Mapping[str, Any]]) -> list[Point]:
    points: list[Point] = []
    for record in records:
        point = Point(str(record["measurement"]))
        for key, value in record["tags"].items():
            point = point.tag(key, value)
        for key, value in record["fields"].items():
            point = point.field(key, value)
        points.append(
            point.time(int(record["timestamp_ns"]), write_precision=WritePrecision.NS)
        )
    return points


def influx_points(payload: Any) -> list[Point]:
    return records_to_points(measurement_records(payload))


def read_token(token: str | None, token_file: str | None) -> str:
    if token_file:
        value = Path(token_file).read_text(encoding="utf-8").strip()
    else:
        value = (token or "").strip()
    if not value:
        raise ValueError("INFLUX_TOKEN or INFLUX_TOKEN_FILE is required")
    return value


def _labels(value: str) -> tuple[str, ...]:
    return tuple(item.strip() for item in value.split(",") if item.strip())


def _boolean(value: str) -> bool:
    normalized = value.strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    raise argparse.ArgumentTypeError(f"invalid boolean value: {value!r}")


def window_seconds_from_environment(
    environment: Mapping[str, str] | None = None,
) -> float:
    """Prefer the current setting while preserving the TIME_INTERVAL alias."""
    values = os.environ if environment is None else environment
    raw = values.get("MONITORING_WINDOW_SECONDS")
    if raw is None:
        raw = values.get("TIME_INTERVAL", "5")
    return float(raw)


class Telemetry:
    def __init__(
        self,
        *,
        kafka_url: str,
        consumer_topic: str,
        consumer_client_id: str,
        consumer_group_id: str,
        influx_url: str,
        influx_bucket: str,
        influx_org: str,
        influx_token: str,
        window_seconds: float = 5.0,
        allowed_lateness_seconds: float = 5.0,
        benign_hh_labels: Sequence[str] = ("benign_heavy_hitter",),
        malign_labels: Sequence[str] = ("malign_heavy_hitter",),
        monitored_labels: Sequence[str] = (
            "normal_traffic",
            "benign_heavy_hitter",
            "malign_heavy_hitter",
        ),
        rebase_event_time: bool = True,
        idle_flush_seconds: float = 1.0,
        max_poll_records: int = 500,
        interval_csv_path: str | None = None,
        latency_csv_path: str | None = None,
    ) -> None:
        self.bucket = influx_bucket
        self.org = influx_org
        self._stopping = False
        self.consumer = KafkaConsumer(
            consumer_topic,
            bootstrap_servers=kafka_url,
            client_id=consumer_client_id,
            group_id=consumer_group_id,
            auto_offset_reset="earliest",
            enable_auto_commit=False,
            max_poll_records=max_poll_records,
        )
        self.influx = InfluxDBClient(
            url=influx_url,
            token=influx_token,
            org=influx_org,
            timeout=30_000,
        )
        self.writer = self.influx.write_api(write_options=SYNCHRONOUS)
        self.aggregator = EventWindowAggregator(
            window_seconds=window_seconds,
            allowed_lateness_seconds=allowed_lateness_seconds,
            benign_hh_labels=benign_hh_labels,
            malign_labels=malign_labels,
            monitored_labels=monitored_labels,
            rebase_event_time=rebase_event_time,
            idle_flush_seconds=idle_flush_seconds,
        )
        self.csv_exports = CsvExports(
            interval_path=interval_csv_path,
            latency_path=latency_csv_path,
        )
        signal.signal(signal.SIGINT, self._handle_signal)
        signal.signal(signal.SIGTERM, self._handle_signal)

    def _handle_signal(self, signum: int, _frame: Any) -> None:
        LOGGER.info("Received signal %s; stopping", signum)
        self._stopping = True

    def _write_records(self, records: Sequence[Mapping[str, Any]]) -> None:
        if not records:
            return
        self.writer.write(
            bucket=self.bucket,
            org=self.org,
            record=records_to_points(records),
        )
        self.csv_exports.write_intervals(records)
        self.csv_exports.flush()

    def run(self) -> None:
        LOGGER.info(
            "Writing predictions and %.3fs experiment windows to InfluxDB bucket %s",
            self.aggregator.window_seconds,
            self.bucket,
        )
        try:
            while not self._stopping:
                polled = self.consumer.poll(timeout_ms=1000)
                if not polled:
                    self._write_records(self.aggregator.flush_idle())
                    continue
                for record in iter_polled_records(polled):
                    try:
                        observed_at = time.time()
                        events = validated_events(
                            json_loads_bytes(record.value),
                            observed_at=observed_at,
                        )
                        output_records: list[dict[str, Any]] = []
                        for sequence, event in enumerate(events):
                            output_records.append(
                                _prediction_record(event, sequence=sequence)
                            )
                            completed, disposition = self.aggregator.add_event(
                                event,
                                observed_at=observed_at,
                            )
                            output_records.extend(completed)
                            self.csv_exports.write_latency(event, disposition)
                        self._write_records(output_records)
                        LOGGER.info("Monitored %d prediction(s)", len(events))
                    except (TelemetryError, TypeError, ValueError) as exc:
                        LOGGER.error(
                            "Rejected Kafka record at %s[%s] offset %s: %s",
                            record.topic,
                            record.partition,
                            record.offset,
                            exc,
                        )
                    except Exception:
                        LOGGER.exception(
                            "Telemetry write failed for %s[%s] offset %s",
                            record.topic,
                            record.partition,
                            record.offset,
                        )
                        raise
                    commit_processed_record(self.consumer, record)
        finally:
            try:
                self._write_records(self.aggregator.flush())
            finally:
                self.consumer.close()
                self.csv_exports.close()
                self.writer.close()
                self.influx.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--kafka-url", default=os.getenv("KAFKA_URL", "kafka:9092"))
    parser.add_argument(
        "--consumer-topic",
        default=os.getenv("CONSUMER_TOPIC", "predicted_labels"),
    )
    parser.add_argument(
        "--consumer-client-id",
        default=os.getenv("CONSUMER_CLIENT_ID", "telemetry-consumer"),
    )
    parser.add_argument(
        "--consumer-group-id",
        default=os.getenv("CONSUMER_GROUP_ID", "telemetry"),
    )
    parser.add_argument(
        "--max-poll-records",
        type=int,
        default=int(os.getenv("MAX_POLL_RECORDS", "500")),
    )
    parser.add_argument(
        "--influx-url",
        default=os.getenv("INFLUX_URL", "http://influxdb:8086"),
    )
    parser.add_argument(
        "--influx-bucket",
        default=os.getenv("INFLUX_BUCKET", "monitoring"),
    )
    parser.add_argument("--influx-org", default=os.getenv("INFLUX_ORG", "tc35"))
    parser.add_argument(
        "--window-seconds",
        type=float,
        default=window_seconds_from_environment(),
    )
    parser.add_argument(
        "--allowed-lateness-seconds",
        type=float,
        default=float(os.getenv("ALLOWED_LATENESS_SECONDS", "5")),
    )
    parser.add_argument(
        "--idle-flush-seconds",
        type=float,
        default=float(os.getenv("IDLE_FLUSH_SECONDS", "1")),
    )
    parser.add_argument(
        "--rebase-event-time",
        type=_boolean,
        default=_boolean(os.getenv("REBASE_EVENT_TIME", "true")),
    )
    parser.add_argument(
        "--benign-hh-labels",
        type=_labels,
        default=_labels(os.getenv("BENIGN_HH_LABELS", "benign_heavy_hitter")),
    )
    parser.add_argument(
        "--malign-labels",
        type=_labels,
        default=_labels(os.getenv("MALIGN_LABELS", "malign_heavy_hitter")),
    )
    parser.add_argument(
        "--monitored-labels",
        type=_labels,
        default=_labels(
            os.getenv(
                "MONITORED_LABELS",
                "normal_traffic,benign_heavy_hitter,malign_heavy_hitter",
            )
        ),
    )
    parser.add_argument(
        "--interval-csv-path",
        default=os.getenv("INTERVAL_CSV_PATH") or None,
    )
    parser.add_argument(
        "--latency-csv-path",
        default=os.getenv("LATENCY_CSV_PATH") or None,
    )
    return parser.parse_args()


def main() -> None:
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s %(levelname)-8s %(name)s: %(message)s",
    )
    args = parse_args()
    telemetry = Telemetry(
        kafka_url=args.kafka_url,
        consumer_topic=args.consumer_topic,
        consumer_client_id=args.consumer_client_id,
        consumer_group_id=args.consumer_group_id,
        max_poll_records=args.max_poll_records,
        influx_url=args.influx_url,
        influx_bucket=args.influx_bucket,
        influx_org=args.influx_org,
        influx_token=read_token(
            os.getenv("INFLUX_TOKEN"),
            os.getenv("INFLUX_TOKEN_FILE"),
        ),
        window_seconds=args.window_seconds,
        allowed_lateness_seconds=args.allowed_lateness_seconds,
        benign_hh_labels=args.benign_hh_labels,
        malign_labels=args.malign_labels,
        monitored_labels=args.monitored_labels,
        rebase_event_time=args.rebase_event_time,
        idle_flush_seconds=args.idle_flush_seconds,
        interval_csv_path=args.interval_csv_path,
        latency_csv_path=args.latency_csv_path,
    )
    telemetry.run()


if __name__ == "__main__":
    main()
