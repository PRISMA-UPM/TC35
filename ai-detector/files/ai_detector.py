"""Convert inference probabilities into classified TC35 events."""

from __future__ import annotations

import argparse
import csv
import json
import logging
import math
import os
import re
import signal
import socket
import time
from collections.abc import Mapping, Sequence
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from kafka import KafkaConsumer, KafkaProducer

from tc35_contract import (
    SCHEMA_VERSION,
    iter_polled_records,
    json_dumps_bytes,
    json_loads_bytes,
)
from tc35_contract.kafka import commit_processed_record

LOGGER = logging.getLogger("tc35.ai_detector")


class DetectionError(ValueError):
    pass


class AuditCsvError(RuntimeError):
    pass


AUDIT_CSV_COLUMNS = (
    "capture_timestamp",
    "src_ip",
    "dst_ip",
    "src_port",
    "dst_port",
    "protocol",
    "label",
    "probabilities",
    "features",
)


def _safe_filename_component(value: str) -> str:
    """Return a portable filename component for a detector replica."""
    safe = re.sub(r"[^A-Za-z0-9_.-]+", "-", value.strip()).strip("-.")
    return safe[:80] or "detector"


class DetectionCsvWriter:
    """Write the detector's per-snapshot audit CSV."""

    def __init__(
        self,
        output_dir: str | os.PathLike[str],
        *,
        instance_id: str,
        hostname: str | None = None,
    ) -> None:
        directory = Path(output_dir)
        directory.mkdir(parents=True, exist_ok=True)

        replica = _safe_filename_component(
            f"{instance_id}-{hostname or socket.gethostname()}"
        )
        started_at = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S.%fZ")
        filename = f"detections_{replica}_{started_at}_{os.getpid()}.csv"
        self.path = directory / filename
        self._file = self.path.open("w", encoding="utf-8", newline="")
        self._writer = csv.DictWriter(
            self._file,
            fieldnames=AUDIT_CSV_COLUMNS,
            delimiter=";",
            lineterminator="\n",
            extrasaction="raise",
        )
        self._writer.writeheader()
        self._file.flush()
        self._closed = False

    def write_batch(
        self,
        inference_payload: Mapping[str, Any],
        classified_payload: Mapping[str, Any],
    ) -> None:
        """Write and flush one audit row for every prediction in a Kafka record."""
        probabilities = inference_payload.get("data")
        labels = classified_payload.get("data")
        metadata = classified_payload.get("metadata")
        if not isinstance(probabilities, list):
            raise AuditCsvError("inference data must be a probability matrix")
        if not isinstance(labels, list) or not isinstance(metadata, list):
            raise AuditCsvError("classified data and metadata must be lists")
        if len(probabilities) != len(labels) or len(labels) != len(metadata):
            raise AuditCsvError("audit batch lengths do not match")

        rows: list[dict[str, Any]] = []
        required_metadata = (
            "src_ip",
            "dst_ip",
            "src_port",
            "dst_port",
            "protocol",
        )
        for probability, label, item in zip(
            probabilities,
            labels,
            metadata,
            strict=True,
        ):
            if not isinstance(item, Mapping):
                raise AuditCsvError("audit metadata entries must be objects")
            if "timestamp" not in item:
                raise AuditCsvError("audit metadata is missing fields: timestamp")
            raw_connection = item.get("connection_id")
            connection = raw_connection if isinstance(raw_connection, Mapping) else {}
            missing = [
                name
                for name in required_metadata
                if name not in connection and name not in item
            ]
            if missing:
                raise AuditCsvError(
                    f"audit metadata is missing fields: {', '.join(missing)}"
                )
            features = item.get("features")
            if not isinstance(features, Mapping):
                raise AuditCsvError("audit metadata features must be an object")

            rows.append(
                {
                    "capture_timestamp": item["timestamp"],
                    "src_ip": connection.get("src_ip", item.get("src_ip")),
                    "dst_ip": connection.get("dst_ip", item.get("dst_ip")),
                    "src_port": connection.get("src_port", item.get("src_port")),
                    "dst_port": connection.get("dst_port", item.get("dst_port")),
                    "protocol": connection.get("protocol", item.get("protocol")),
                    "label": label,
                    "probabilities": json.dumps(
                        probability,
                        ensure_ascii=False,
                        separators=(",", ":"),
                    ),
                    "features": json.dumps(
                        dict(features),
                        ensure_ascii=False,
                        separators=(",", ":"),
                    ),
                }
            )

        self._writer.writerows(rows)
        self._file.flush()

    def close(self) -> None:
        if self._closed:
            return
        self._file.flush()
        self._file.close()
        self._closed = True

    def __enter__(self) -> "DetectionCsvWriter":
        return self

    def __exit__(self, _exc_type: Any, _exc: Any, _traceback: Any) -> None:
        self.close()


def labels_from_probabilities(
    probabilities: Any,
    correspondence: Any,
) -> tuple[list[str], list[float]]:
    if not isinstance(probabilities, list) or not probabilities:
        raise DetectionError("data must be a non-empty probability matrix")
    if not isinstance(correspondence, Mapping) or not correspondence:
        raise DetectionError("label_correspondence must be a non-empty object")

    labels: list[str] = []
    confidences: list[float] = []
    for row in probabilities:
        if not isinstance(row, Sequence) or isinstance(row, (str, bytes)) or not row:
            raise DetectionError("each probability row must be a non-empty list")
        normalized: list[float] = []
        for value in row:
            if isinstance(value, bool) or not isinstance(value, (int, float)):
                raise DetectionError("probabilities must be numeric")
            probability = float(value)
            if not math.isfinite(probability) or probability < 0 or probability > 1:
                raise DetectionError("probabilities must be finite values in [0, 1]")
            normalized.append(probability)

        class_index = max(range(len(normalized)), key=normalized.__getitem__)
        label = correspondence.get(str(class_index), correspondence.get(class_index))
        if label is None:
            raise DetectionError(f"no label is defined for class index {class_index}")
        labels.append(str(label))
        confidences.append(normalized[class_index])
    return labels, confidences


def classify_payload(payload: Any, now: float | None = None) -> dict[str, Any]:
    if not isinstance(payload, Mapping):
        raise DetectionError("inference payload must be an object")
    version = payload.get("schema_version")
    if version != SCHEMA_VERSION:
        raise DetectionError(f"unsupported schema version {version!r}")

    labels, confidences = labels_from_probabilities(
        payload.get("data"),
        payload.get("label_correspondence"),
    )
    raw_metadata = payload.get("metadata")
    if not isinstance(raw_metadata, list) or len(raw_metadata) != len(labels):
        raise DetectionError("metadata length must equal the number of predictions")

    detection_time = time.time() if now is None else now
    metadata: list[dict[str, Any]] = []
    for item, label, confidence in zip(
        raw_metadata,
        labels,
        confidences,
        strict=True,
    ):
        if not isinstance(item, Mapping):
            raise DetectionError("each metadata entry must be an object")
        enriched = dict(item)
        enriched["label"] = label
        enriched["ml_confidence"] = confidence
        enriched["timestamp_detector"] = detection_time
        metadata.append(enriched)

    return {
        "schema_version": SCHEMA_VERSION,
        "model": payload.get("model"),
        "data": labels,
        "metadata": metadata,
    }


class AIDetector:
    def __init__(
        self,
        *,
        kafka_url: str,
        consumer_topic: str,
        producer_topic: str,
        consumer_client_id: str,
        producer_client_id: str,
        consumer_group_id: str,
        csv_output_dir: str | None = None,
    ) -> None:
        self.consumer_topic = consumer_topic
        self.producer_topic = producer_topic
        self._stopping = False
        self.producer = KafkaProducer(
            bootstrap_servers=kafka_url,
            client_id=f"{producer_client_id}-{socket.gethostname()}",
            value_serializer=json_dumps_bytes,
            acks="all",
            retries=10,
        )
        self.consumer = KafkaConsumer(
            consumer_topic,
            bootstrap_servers=kafka_url,
            client_id=f"{consumer_client_id}-{socket.gethostname()}",
            group_id=consumer_group_id,
            auto_offset_reset="earliest",
            enable_auto_commit=False,
        )
        normalized_csv_dir = (csv_output_dir or "").strip()
        try:
            self.audit_writer = (
                DetectionCsvWriter(
                    normalized_csv_dir,
                    instance_id=consumer_client_id,
                )
                if normalized_csv_dir
                else None
            )
        except Exception:
            self.consumer.close()
            self.producer.close(timeout=30)
            raise
        signal.signal(signal.SIGINT, self._handle_signal)
        signal.signal(signal.SIGTERM, self._handle_signal)

    def _handle_signal(self, signum: int, _frame: Any) -> None:
        LOGGER.info("Received signal %s; stopping", signum)
        self._stopping = True

    def run(self) -> None:
        LOGGER.info(
            "Consuming %s and publishing labels to %s",
            self.consumer_topic,
            self.producer_topic,
        )
        try:
            while not self._stopping:
                polled = self.consumer.poll(timeout_ms=1000)
                for record in iter_polled_records(polled):
                    try:
                        payload = json_loads_bytes(record.value)
                        output = classify_payload(payload)
                        if self.audit_writer is not None:
                            self.audit_writer.write_batch(payload, output)
                        self.producer.send(
                            self.producer_topic,
                            value=output,
                            headers=[
                                ("version", SCHEMA_VERSION.encode("utf-8")),
                            ],
                        ).get(timeout=30)
                        LOGGER.info("Classified %d snapshot(s)", len(output["data"]))
                    except (DetectionError, KeyError, TypeError, ValueError) as exc:
                        LOGGER.error(
                            "Rejected Kafka record at %s[%s] offset %s: %s",
                            record.topic,
                            record.partition,
                            record.offset,
                            exc,
                        )
                    except Exception:
                        LOGGER.exception(
                            "Detection failed for %s[%s] offset %s",
                            record.topic,
                            record.partition,
                            record.offset,
                        )
                        raise
                    commit_processed_record(self.consumer, record)
        finally:
            try:
                if self.audit_writer is not None:
                    self.audit_writer.close()
            finally:
                try:
                    self.consumer.close()
                finally:
                    try:
                        self.producer.flush(timeout=30)
                    finally:
                        self.producer.close(timeout=30)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--kafka-url", default=os.getenv("KAFKA_URL", "kafka:9092"))
    parser.add_argument(
        "--consumer-topic",
        default=os.getenv("CONSUMER_TOPIC", "inference_probs"),
    )
    parser.add_argument(
        "--producer-topic",
        default=os.getenv("PRODUCER_TOPIC", "predicted_labels"),
    )
    parser.add_argument(
        "--consumer-client-id",
        default=os.getenv("CONSUMER_CLIENT_ID", "ai-detector-consumer"),
    )
    parser.add_argument(
        "--producer-client-id",
        default=os.getenv("PRODUCER_CLIENT_ID", "ai-detector-producer"),
    )
    parser.add_argument(
        "--consumer-group-id",
        default=os.getenv("CONSUMER_GROUP_ID", "ai-detector"),
    )
    parser.add_argument(
        "--csv-output-dir",
        default=os.getenv("DETECTOR_CSV_DIR"),
        help="optional directory for per-snapshot detector audit CSV files",
    )
    return parser.parse_args()


def main() -> None:
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s %(levelname)-8s %(name)s: %(message)s",
    )
    args = parse_args()
    detector = AIDetector(
        kafka_url=args.kafka_url,
        consumer_topic=args.consumer_topic,
        producer_topic=args.producer_topic,
        consumer_client_id=args.consumer_client_id,
        producer_client_id=args.producer_client_id,
        consumer_group_id=args.consumer_group_id,
        csv_output_dir=args.csv_output_dir,
    )
    detector.run()


if __name__ == "__main__":
    main()
