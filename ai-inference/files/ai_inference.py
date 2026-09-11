"""Load a checksum-pinned model and run inference on NFStream snapshots."""

from __future__ import annotations

import argparse
import base64
import hashlib
import logging
import os
import signal
import socket
import tempfile
import time
from collections import deque
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from math import isfinite
from pathlib import Path
from typing import Any
from urllib.parse import quote

import joblib
import numpy as np
import pandas as pd
import requests
from kafka import KafkaConsumer, KafkaProducer
from kafka.structs import OffsetAndMetadata, TopicPartition

from tc35_contract import (
    ContractError,
    FEATURE_NAMES_V1,
    SNAPSHOT_INTERVAL_SECONDS_V1,
    extract_snapshot_batch,
    iter_polled_records,
    json_dumps_bytes,
    json_loads_bytes,
)

LOGGER = logging.getLogger("tc35.ai_inference")
REQUEST_TIMEOUT_SECONDS = 30
MODEL_DISCOVERY_RETRY_SECONDS = 2
DEFAULT_BATCH_SIZE = 900
DEFAULT_IDLE_POLL_SECONDS = 0.001


@dataclass(frozen=True)
class ModelRecord:
    document_id: str
    filename: str
    metadata: dict[str, Any]
    sha256: str | None


@dataclass(eq=False)
class _TrackedRecord:
    record: Any
    complete: bool = False


@dataclass
class _PendingInput:
    tracked: _TrackedRecord
    remaining_snapshots: int


class _ContiguousOffsetTracker:
    """Commit only completed prefixes of the records seen in each partition."""

    def __init__(self, consumer: Any) -> None:
        self.consumer = consumer
        self._pending: dict[tuple[str, int], deque[_TrackedRecord]] = {}

    @staticmethod
    def _key(record: Any) -> tuple[str, int]:
        return str(record.topic), int(record.partition)

    def observe(self, record: Any) -> _TrackedRecord:
        key = self._key(record)
        queue = self._pending.setdefault(key, deque())
        if queue and int(record.offset) <= int(queue[-1].record.offset):
            raise ValueError(
                "Kafka records must be observed in increasing offset order "
                "per partition"
            )
        tracked = _TrackedRecord(record=record)
        queue.append(tracked)
        return tracked

    def mark_completed(self, records: Sequence[_TrackedRecord]) -> None:
        affected: set[tuple[str, int]] = set()
        for tracked in records:
            tracked.complete = True
            affected.add(self._key(tracked.record))

        offsets: dict[TopicPartition, OffsetAndMetadata] = {}
        pop_counts: dict[tuple[str, int], int] = {}
        for key in affected:
            queue = self._pending[key]
            count = 0
            last_record = None
            for tracked in queue:
                if not tracked.complete:
                    break
                count += 1
                last_record = tracked.record
            if last_record is None:
                continue
            offsets[TopicPartition(*key)] = OffsetAndMetadata(
                int(last_record.offset) + 1,
                "",
                -1,
            )
            pop_counts[key] = count

        if not offsets:
            return

        # Mutate the queues only after Kafka acknowledges the commit. If this
        # raises, the caller exits and Kafka will redeliver the uncommitted input.
        self.consumer.commit(offsets=offsets)
        for key, count in pop_counts.items():
            queue = self._pending[key]
            for _ in range(count):
                queue.popleft()
            if not queue:
                del self._pending[key]


def _catalog_url(value: str) -> str:
    return value.rstrip("/") if "://" in value else f"http://{value.rstrip('/')}"


def _safe_filename(filename: str) -> str:
    if not filename or Path(filename).name != filename or "\\" in filename:
        raise ValueError(f"unsafe model filename: {filename!r}")
    return filename


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def model_feature_names(model: Any, metadata: dict[str, Any]) -> tuple[str, ...]:
    """Resolve and cross-check the feature schema stored with a model."""
    metadata_names = metadata.get("feature_names")
    if metadata_names is None:
        from_metadata: tuple[str, ...] = ()
    elif isinstance(metadata_names, Sequence) and not isinstance(
        metadata_names,
        (str, bytes, bytearray),
    ):
        from_metadata = tuple(str(name) for name in metadata_names)
    else:
        raise ValueError("model metadata feature_names must be a list")
    model_names_raw = getattr(model, "feature_names_in_", ())
    from_model = tuple(str(name) for name in model_names_raw)

    if from_metadata and from_model and from_metadata != from_model:
        raise ValueError(
            f"model metadata schema {from_metadata!r} differs from model schema {from_model!r}"
        )
    names = from_model or from_metadata
    if not names:
        raise ValueError("model does not declare feature names")
    if names != FEATURE_NAMES_V1:
        raise ValueError(f"model schema is not the supported NFStream v1 schema: {names!r}")

    expected_count = getattr(model, "n_features_in_", len(names))
    if int(expected_count) != len(names):
        raise ValueError(
            f"model declares n_features_in_={expected_count}, but {len(names)} names were found"
        )
    return names


def model_snapshot_interval(metadata: dict[str, Any]) -> float:
    value = metadata.get("snapshot_interval_seconds")
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError("model metadata must declare snapshot_interval_seconds")
    interval = float(value)
    if interval != SNAPSHOT_INTERVAL_SECONDS_V1:
        raise ValueError(
            "model snapshot interval is not the supported NFStream v1 interval: "
            f"{interval!r}"
        )
    return interval


def model_label_correspondence(
    model: Any,
    metadata: dict[str, Any],
) -> dict[str, str]:
    labels = metadata.get("label_correspondence")
    if not isinstance(labels, Mapping) or not labels:
        raise ValueError("model metadata must contain label_correspondence")
    normalized = {str(key): str(value) for key, value in labels.items()}

    raw_classes = getattr(model, "classes_", None)
    if raw_classes is None:
        raise ValueError("model does not declare classes_")
    classes = tuple(str(getattr(value, "item", lambda: value)()) for value in raw_classes)
    expected_classes = tuple(str(index) for index in range(len(classes)))
    if classes != expected_classes:
        raise ValueError(
            "model classes must be contiguous integer IDs in probability-column order: "
            f"{classes!r}"
        )
    if set(normalized) != set(classes):
        raise ValueError(
            "label_correspondence keys do not match model classes: "
            f"{tuple(normalized)!r} != {classes!r}"
        )
    return normalized


def prepare_metadata(
    metadata: list[dict[str, Any]],
    rows: list[dict[str, Any]],
    now: float | None = None,
) -> list[dict[str, Any]]:
    """Stamp receipt time and normalize NFStream's millisecond capture time."""
    timestamp_inference = time.time() if now is None else now
    prepared: list[dict[str, Any]] = []
    for item, features in zip(metadata, rows, strict=True):
        result = dict(item)
        capture_time = result.get("timestamp")
        if isinstance(capture_time, (int, float)) and capture_time > 10_000_000_000:
            result["timestamp"] = capture_time / 1000.0
        result["features"] = dict(features)
        result["timestamp_inference"] = timestamp_inference
        prepared.append(result)
    return prepared


class AIInference:
    def __init__(
        self,
        kafka_url: str,
        catalog_url: str,
        models_index: str,
        producer_client_id: str,
        consumer_client_id: str,
        consumer_group_id: str,
        consumer_topic: str,
        producer_topic: str,
        load_model: str,
        model_sha256: str | None,
        models_path: str,
        batch_size: int = DEFAULT_BATCH_SIZE,
        idle_poll_seconds: float = DEFAULT_IDLE_POLL_SECONDS,
    ) -> None:
        if (
            isinstance(batch_size, bool)
            or not isinstance(batch_size, int)
            or batch_size <= 0
        ):
            raise ValueError("batch_size must be a positive integer")
        try:
            normalized_idle_poll = float(idle_poll_seconds)
        except (TypeError, ValueError) as exc:
            raise ValueError(
                "idle_poll_seconds must be finite and non-negative"
            ) from exc
        if (
            isinstance(idle_poll_seconds, bool)
            or not isfinite(normalized_idle_poll)
            or normalized_idle_poll < 0
        ):
            raise ValueError("idle_poll_seconds must be finite and non-negative")

        self.catalog_url = _catalog_url(catalog_url)
        self.models_index = models_index
        self.consumer_topic = consumer_topic
        self.producer_topic = producer_topic
        self.configured_sha256 = model_sha256.lower() if model_sha256 else None
        self.models_path = Path(models_path)
        self.models_path.mkdir(parents=True, exist_ok=True)
        self.session = requests.Session()
        self._stopping = False
        self.batch_size = batch_size
        self.idle_poll_seconds = normalized_idle_poll

        record = self.wait_for_model(load_model)
        model_path, artifact_sha256 = self.download_model(record)
        self.model = joblib.load(model_path)
        self.feature_names = model_feature_names(self.model, record.metadata)
        self.snapshot_interval_seconds = model_snapshot_interval(record.metadata)
        self.model_record = record
        self.model_sha256 = artifact_sha256
        self.label_correspondence = model_label_correspondence(
            self.model,
            record.metadata,
        )

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
        self._offset_tracker = _ContiguousOffsetTracker(self.consumer)
        self._batch_rows: list[dict[str, Any]] = []
        self._batch_metadata: list[dict[str, Any]] = []
        self._batch_contributions: list[tuple[_PendingInput, int]] = []
        self._batch_version: str | None = None

        signal.signal(signal.SIGINT, self._handle_signal)
        signal.signal(signal.SIGTERM, self._handle_signal)
        LOGGER.info(
            "Loaded model %s (%s) with SHA-256 %s",
            record.filename,
            record.document_id,
            artifact_sha256,
        )
        LOGGER.info(
            "Inference batching: up to %d snapshots; %.3fs sleep after an empty poll",
            self.batch_size,
            self.idle_poll_seconds,
        )

    def _handle_signal(self, signum: int, _frame: Any) -> None:
        LOGGER.info("Received signal %s; stopping", signum)
        self._stopping = True

    def list_models(self) -> list[ModelRecord]:
        index = quote(self.models_index, safe="")
        url = f"{self.catalog_url}/{index}/_search"
        payload = {
            "size": 1000,
            "_source": [
                "file_data.filename",
                "file_data.sha256",
                "metadata",
                "uploaded_at",
            ],
            "sort": [{"uploaded_at": {"order": "desc", "unmapped_type": "date"}}],
        }
        response = self.session.post(
            url,
            json=payload,
            timeout=REQUEST_TIMEOUT_SECONDS,
        )
        response.raise_for_status()
        response_payload = response.json()
        if not isinstance(response_payload, Mapping):
            raise ValueError("model repository returned a non-object response")
        hit_container = response_payload.get("hits", {})
        if not isinstance(hit_container, Mapping):
            raise ValueError("model repository returned an invalid hits object")
        hits = hit_container.get("hits", [])
        if not isinstance(hits, list):
            raise ValueError("model repository returned an invalid hits list")

        records: list[ModelRecord] = []
        for hit in hits:
            if not isinstance(hit, Mapping):
                LOGGER.warning("Skipping malformed model search hit")
                continue
            document_id = str(hit.get("_id", ""))
            source = hit.get("_source", {})
            if not isinstance(source, Mapping):
                LOGGER.warning("Skipping model %s with invalid source", document_id)
                continue
            file_data = source.get("file_data", {})
            if not isinstance(file_data, Mapping):
                LOGGER.warning("Skipping model %s with invalid file data", document_id)
                continue
            try:
                filename = _safe_filename(str(file_data.get("filename", "")))
            except ValueError as exc:
                LOGGER.warning("Skipping model %s: %s", document_id, exc)
                continue
            metadata = source.get("metadata")
            if not isinstance(metadata, dict):
                LOGGER.warning("Skipping model %s with invalid metadata", document_id)
                continue
            records.append(
                ModelRecord(
                    document_id=document_id,
                    filename=filename,
                    metadata=metadata,
                    sha256=file_data.get("sha256") or metadata.get("artifact_sha256"),
                )
            )
        return records

    def wait_for_model(self, selector: str) -> ModelRecord:
        while True:
            try:
                models = self.list_models()
                selected = next(
                    (
                        model
                        for model in models
                        if selector in (model.document_id, model.filename)
                    ),
                    None,
                )
                if selected is not None:
                    return selected
                LOGGER.info(
                    "Model %s is not available yet; retrying in %ss",
                    selector,
                    MODEL_DISCOVERY_RETRY_SECONDS,
                )
            except (requests.RequestException, KeyError, TypeError, ValueError) as exc:
                LOGGER.warning(
                    "Could not query the model repository (%s); retrying in %ss",
                    exc,
                    MODEL_DISCOVERY_RETRY_SECONDS,
                )
            time.sleep(MODEL_DISCOVERY_RETRY_SECONDS)

    def download_model(self, record: ModelRecord) -> tuple[Path, str]:
        index = quote(self.models_index, safe="")
        document_id = quote(record.document_id, safe="")
        url = f"{self.catalog_url}/{index}/_doc/{document_id}"
        response = self.session.get(url, timeout=REQUEST_TIMEOUT_SECONDS)
        response.raise_for_status()
        source = response.json()["_source"]
        file_data = source["file_data"]

        filename = _safe_filename(str(file_data["filename"]))
        if filename != record.filename:
            raise ValueError("model filename changed between discovery and download")
        if Path(filename).suffix.lower() != ".joblib":
            raise ValueError("only checksum-pinned .joblib models are supported")

        try:
            artifact = base64.b64decode(file_data["file_content"], validate=True)
        except (KeyError, ValueError) as exc:
            raise ValueError("model repository returned invalid base64 content") from exc
        actual_sha256 = _sha256(artifact)
        recorded_sha256 = str(
            file_data.get("sha256")
            or source.get("metadata", {}).get("artifact_sha256")
            or ""
        ).lower()

        if not recorded_sha256:
            raise ValueError("model repository entry has no SHA-256")
        if actual_sha256 != recorded_sha256:
            raise ValueError(
                f"downloaded model checksum {actual_sha256} differs from repository metadata"
            )
        if record.document_id.lower() != actual_sha256:
            raise ValueError(
                "model document ID is not the artifact SHA-256: "
                f"{record.document_id!r}"
            )
        if self.configured_sha256 and actual_sha256 != self.configured_sha256:
            raise ValueError(
                f"downloaded model checksum {actual_sha256} differs from configured checksum"
            )

        target = self.models_path / filename
        temporary_name: str | None = None
        try:
            with tempfile.NamedTemporaryFile(
                mode="wb",
                dir=self.models_path,
                prefix=".model-",
                suffix=".tmp",
                delete=False,
            ) as temporary:
                temporary.write(artifact)
                temporary.flush()
                os.fsync(temporary.fileno())
                temporary_name = temporary.name
            os.replace(temporary_name, target)
        finally:
            if temporary_name and os.path.exists(temporary_name):
                os.unlink(temporary_name)
        return target, actual_sha256

    def _publish_batch(
        self,
        rows: list[dict[str, Any]],
        metadata: list[dict[str, Any]],
        version: str,
    ) -> None:
        """Run one vectorized prediction and durably publish one Kafka record."""
        frame = pd.DataFrame.from_records(rows, columns=self.feature_names)
        probabilities = np.asarray(self.model.predict_proba(frame), dtype=float)
        if probabilities.ndim != 2 or probabilities.shape[0] != len(rows):
            raise ValueError("model returned an invalid probability matrix")
        if not np.isfinite(probabilities).all():
            raise ValueError("model returned non-finite probabilities")

        output = {
            "schema_version": version,
            "model": {
                "id": self.model_record.document_id,
                "filename": self.model_record.filename,
                "sha256": self.model_sha256,
            },
            "data": probabilities.tolist(),
            "metadata": metadata,
            "label_correspondence": self.label_correspondence,
        }
        self.producer.send(
            self.producer_topic,
            value=output,
            headers=[("version", version.encode("utf-8"))],
        ).get(timeout=30)
        LOGGER.info("Predicted %d NFStream snapshot(s)", len(rows))

    def _flush_batch(self) -> bool:
        """Publish the current batch, then complete its consumed input records."""
        if not self._batch_rows:
            return False

        rows = self._batch_rows
        metadata = self._batch_metadata
        contributions = self._batch_contributions
        version = self._batch_version
        if version is None:
            raise RuntimeError("buffered inference batch has no schema version")

        # Do not alter the buffer or its offsets until the output broker has
        # acknowledged the corresponding probability record.
        self._publish_batch(rows, metadata, version)

        self._batch_rows = []
        self._batch_metadata = []
        self._batch_contributions = []
        self._batch_version = None

        completed: list[_TrackedRecord] = []
        for pending, snapshot_count in contributions:
            pending.remaining_snapshots -= snapshot_count
            if pending.remaining_snapshots < 0:
                raise RuntimeError("inference batch over-completed an input record")
            if pending.remaining_snapshots == 0:
                completed.append(pending.tracked)
        self._offset_tracker.mark_completed(completed)
        return True

    def _enqueue_input(
        self,
        rows: list[dict[str, Any]],
        metadata: list[dict[str, Any]],
        version: str,
        tracked: _TrackedRecord,
    ) -> None:
        """Add one validated Kafka input, splitting legacy batches as needed."""
        if len(rows) != len(metadata) or not rows:
            raise ValueError("validated input must contain matching, non-empty rows")

        # Stamp each snapshot as it enters inference, before it waits in the
        # 900-row buffer and before predict_proba(). Preserve that
        # latency boundary instead of moving buffering/model time downstream.
        prepared_metadata = prepare_metadata(metadata, rows)
        pending = _PendingInput(tracked=tracked, remaining_snapshots=len(rows))
        position = 0
        while position < len(rows):
            if self._batch_rows and self._batch_version != version:
                self._flush_batch()
            if not self._batch_rows:
                self._batch_version = version

            capacity = self.batch_size - len(self._batch_rows)
            count = min(capacity, len(rows) - position)
            end = position + count
            self._batch_rows.extend(rows[position:end])
            self._batch_metadata.extend(prepared_metadata[position:end])
            self._batch_contributions.append((pending, count))
            position = end

            if len(self._batch_rows) == self.batch_size:
                self._flush_batch()

    def run(self) -> None:
        LOGGER.info(
            "Consuming %s and publishing probabilities to %s",
            self.consumer_topic,
            self.producer_topic,
        )
        failed = False
        try:
            while not self._stopping:
                # A non-blocking poll lets continuous
                # backlog accumulated to 900 rows, while the first empty poll
                # flushed a partial batch.  A wall-clock deadline here would
                # fragment batches during the high-rate 25 GB replay.
                records = list(iter_polled_records(self.consumer.poll(timeout_ms=0)))
                if not records:
                    self._flush_batch()
                    if self.idle_poll_seconds:
                        time.sleep(self.idle_poll_seconds)
                    continue

                for record in records:
                    tracked = self._offset_tracker.observe(record)
                    try:
                        value = json_loads_bytes(record.value)
                        rows, metadata, version = extract_snapshot_batch(
                            value,
                            record.headers,
                            self.feature_names,
                            self.snapshot_interval_seconds,
                        )
                    except (ContractError, ValueError, TypeError, KeyError) as exc:
                        LOGGER.error(
                            "Rejected Kafka record at %s[%s] offset %s: %s",
                            record.topic,
                            record.partition,
                            record.offset,
                            exc,
                        )
                        # A deliberate rejection is complete, but the tracker
                        # will not commit it past an earlier buffered record.
                        self._offset_tracker.mark_completed([tracked])
                        continue

                    try:
                        self._enqueue_input(rows, metadata, version, tracked)
                    except Exception:
                        LOGGER.exception(
                            "Inference failed for %s[%s] offset %s",
                            record.topic,
                            record.partition,
                            record.offset,
                        )
                        raise
        except BaseException:
            failed = True
            raise
        finally:
            try:
                # Signals set _stopping instead of raising, so normal shutdown
                # durably drains a partial batch before closing the consumer.
                if not failed:
                    self._flush_batch()
            finally:
                self.consumer.close()
                self.producer.flush(timeout=30)
                self.producer.close(timeout=30)
                self.session.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--kafka-url", default=os.getenv("KAFKA_URL", "kafka:9092"))
    parser.add_argument(
        "--catalog-url",
        default=os.getenv("CATALOG_URL", "http://ai-repository:9200"),
    )
    parser.add_argument(
        "--consumer-topic",
        default=os.getenv("CONSUMER_TOPIC", "inference_data"),
    )
    parser.add_argument(
        "--producer-topic",
        default=os.getenv("PRODUCER_TOPIC", "inference_probs"),
    )
    parser.add_argument(
        "--consumer-client-id",
        default=os.getenv("CONSUMER_CLIENT_ID", "ai-inference-consumer"),
    )
    parser.add_argument(
        "--producer-client-id",
        default=os.getenv("PRODUCER_CLIENT_ID", "ai-inference-producer"),
    )
    parser.add_argument(
        "--consumer-group-id",
        default=os.getenv("CONSUMER_GROUP_ID", "ai-inference"),
    )
    parser.add_argument(
        "--catalog-models-index",
        default=os.getenv("CATALOG_MODELS_INDEX", "models"),
    )
    configured_model = os.getenv("LOAD_MODEL")
    parser.add_argument(
        "--load-model",
        default=configured_model,
        required=configured_model is None,
    )
    parser.add_argument("--model-sha256", default=os.getenv("MODEL_SHA256"))
    parser.add_argument("--models-path", default=os.getenv("MODELS_PATH", "./models"))
    parser.add_argument(
        "--batch-size",
        type=int,
        default=os.getenv("INFERENCE_BATCH_SIZE", str(DEFAULT_BATCH_SIZE)),
        help="maximum snapshots per vectorized prediction",
    )
    parser.add_argument(
        "--idle-poll-seconds",
        type=float,
        default=os.getenv(
            "INFERENCE_IDLE_POLL_SECONDS",
            str(DEFAULT_IDLE_POLL_SECONDS),
        ),
        help="sleep after an empty non-blocking poll (which flushes a partial batch)",
    )
    return parser.parse_args()


def main() -> None:
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s %(levelname)-8s %(name)s: %(message)s",
    )
    args = parse_args()
    service = AIInference(
        kafka_url=args.kafka_url,
        catalog_url=args.catalog_url,
        models_index=args.catalog_models_index,
        producer_client_id=args.producer_client_id,
        consumer_client_id=args.consumer_client_id,
        consumer_group_id=args.consumer_group_id,
        consumer_topic=args.consumer_topic,
        producer_topic=args.producer_topic,
        load_model=args.load_model,
        model_sha256=args.model_sha256,
        models_path=args.models_path,
        batch_size=args.batch_size,
        idle_poll_seconds=args.idle_poll_seconds,
    )
    service.run()


if __name__ == "__main__":
    main()
