from __future__ import annotations

import unittest
from types import SimpleNamespace
from typing import Any
from unittest.mock import patch

import numpy as np

from tc35_contract import FEATURE_NAMES_V1, build_snapshot, json_dumps_bytes
from tests.helpers import kafka_import_context, load_module

with kafka_import_context():
    inference = load_module(
        "tc35_ai_inference_batching",
        "ai-inference/files/ai_inference.py",
    )


class FakeFuture:
    def __init__(self, error: Exception | None = None) -> None:
        self.error = error

    def get(self, timeout: int) -> None:
        if self.error is not None:
            raise self.error


class FakeProducer:
    def __init__(self) -> None:
        self.sent: list[dict[str, Any]] = []
        self.error: Exception | None = None
        self.flushed = False
        self.closed = False

    def send(self, topic: str, **kwargs: Any) -> FakeFuture:
        self.sent.append({"topic": topic, **kwargs})
        return FakeFuture(self.error)

    def flush(self, timeout: int) -> None:
        self.flushed = True

    def close(self, timeout: int) -> None:
        self.closed = True


class FakeConsumer:
    def __init__(self) -> None:
        self.commits: list[dict[Any, Any]] = []
        self.closed = False

    def commit(self, offsets: dict[Any, Any]) -> None:
        self.commits.append(offsets)

    def close(self) -> None:
        self.closed = True


class FakeSession:
    def __init__(self) -> None:
        self.closed = False

    def close(self) -> None:
        self.closed = True


class FakeModel:
    def __init__(self) -> None:
        self.batch_lengths: list[int] = []

    def predict_proba(self, frame: Any) -> np.ndarray:
        self.batch_lengths.append(len(frame))
        return np.tile([0.7, 0.2, 0.1], (len(frame), 1))


def kafka_record(offset: int, **kwargs: Any) -> SimpleNamespace:
    values = {
        "topic": "inference_data",
        "partition": 0,
        "offset": offset,
        "headers": [],
        "value": b"",
    }
    values.update(kwargs)
    return SimpleNamespace(**values)


def feature_row(seed: int) -> dict[str, float]:
    return {name: float(seed + index) for index, name in enumerate(FEATURE_NAMES_V1)}


def metadata(seed: int) -> dict[str, Any]:
    return {"timestamp": 1_720_000_000_000 + seed, "src_ip": f"192.0.2.{seed + 1}"}


def make_service(
    batch_size: int = 3,
    idle_poll: float = 0.001,
    consumer: FakeConsumer | None = None,
) -> Any:
    service = object.__new__(inference.AIInference)
    service.feature_names = FEATURE_NAMES_V1
    service.snapshot_interval_seconds = 0.5
    service.model = FakeModel()
    service.model_record = inference.ModelRecord(
        document_id="model-sha",
        filename="model.joblib",
        metadata={},
        sha256="model-sha",
    )
    service.model_sha256 = "model-sha"
    service.label_correspondence = {
        "0": "normal_traffic",
        "1": "benign_heavy_hitter",
        "2": "malign_heavy_hitter",
    }
    service.producer_topic = "inference_probs"
    service.consumer_topic = "inference_data"
    service.batch_size = batch_size
    service.idle_poll_seconds = idle_poll
    service.consumer = consumer or FakeConsumer()
    service.producer = FakeProducer()
    service.session = FakeSession()
    service._offset_tracker = inference._ContiguousOffsetTracker(service.consumer)
    service._batch_rows = []
    service._batch_metadata = []
    service._batch_contributions = []
    service._batch_version = None
    service._stopping = False
    return service


def committed_offset(commit: dict[Any, Any]) -> int:
    return next(iter(commit.values())).offset


class InferenceBatchingTests(unittest.TestCase):
    def test_three_inputs_use_one_vectorized_prediction_and_one_output(self) -> None:
        service = make_service(batch_size=3)

        for offset in range(3):
            tracked = service._offset_tracker.observe(kafka_record(offset))
            service._enqueue_input(
                [feature_row(offset)],
                [metadata(offset)],
                "v1",
                tracked,
            )

        self.assertEqual(service.model.batch_lengths, [3])
        self.assertEqual(len(service.producer.sent), 1)
        self.assertEqual(len(service.producer.sent[0]["value"]["data"]), 3)
        self.assertEqual(len(service.producer.sent[0]["value"]["metadata"]), 3)
        self.assertEqual(len(service.consumer.commits), 1)
        self.assertEqual(committed_offset(service.consumer.commits[0]), 3)

    def test_inference_timestamp_is_stamped_before_buffer_and_prediction(self) -> None:
        service = make_service(batch_size=2)
        first = service._offset_tracker.observe(kafka_record(0))
        second = service._offset_tracker.observe(kafka_record(1))

        with patch.object(inference.time, "time", return_value=101.0):
            service._enqueue_input([feature_row(0)], [metadata(0)], "v1", first)
        with patch.object(inference.time, "time", return_value=202.0):
            service._enqueue_input([feature_row(1)], [metadata(1)], "v1", second)

        output_metadata = service.producer.sent[0]["value"]["metadata"]
        self.assertEqual(
            [item["timestamp_inference"] for item in output_metadata],
            [101.0, 202.0],
        )

    def test_legacy_prebatched_record_is_split_without_early_commit(self) -> None:
        service = make_service(batch_size=3)
        tracked = service._offset_tracker.observe(kafka_record(7))
        rows = [feature_row(index) for index in range(5)]
        metadatas = [metadata(index) for index in range(5)]

        service._enqueue_input(rows, metadatas, "v1", tracked)

        self.assertEqual(service.model.batch_lengths, [3])
        self.assertEqual(service.consumer.commits, [])
        self.assertEqual(len(service._batch_rows), 2)

        service._flush_batch()

        self.assertEqual(service.model.batch_lengths, [3, 2])
        self.assertEqual(len(service.producer.sent), 2)
        self.assertEqual(committed_offset(service.consumer.commits[0]), 8)

    def test_output_failure_does_not_commit_or_discard_batch(self) -> None:
        service = make_service(batch_size=2)
        service.producer.error = RuntimeError("broker rejected output")
        first = service._offset_tracker.observe(kafka_record(0))
        second = service._offset_tracker.observe(kafka_record(1))
        service._enqueue_input([feature_row(0)], [metadata(0)], "v1", first)

        with self.assertRaisesRegex(RuntimeError, "broker rejected"):
            service._enqueue_input([feature_row(1)], [metadata(1)], "v1", second)

        self.assertEqual(service.consumer.commits, [])
        self.assertEqual(len(service._batch_rows), 2)

    def test_rejected_record_cannot_skip_an_earlier_buffered_record(self) -> None:
        consumer = FakeConsumer()
        tracker = inference._ContiguousOffsetTracker(consumer)
        valid = tracker.observe(kafka_record(10))
        rejected = tracker.observe(kafka_record(11))

        tracker.mark_completed([rejected])
        self.assertEqual(consumer.commits, [])

        tracker.mark_completed([valid])
        self.assertEqual(len(consumer.commits), 1)
        self.assertEqual(committed_offset(consumer.commits[0]), 12)

    def test_first_empty_poll_flushes_partial_batch(self) -> None:
        snapshot = build_snapshot(
            feature_row(0),
            {"connection_id": {"src_ip": "192.0.2.1"}, "timestamp": 1},
        )
        record = kafka_record(3, value=json_dumps_bytes(snapshot))

        class EmptyAfterRecordConsumer(FakeConsumer):
            service: Any
            polls = 0

            def poll(self, timeout_ms: int) -> dict[str, list[Any]]:
                self.assert_nonblocking(timeout_ms)
                self.polls += 1
                if self.polls == 1:
                    return {"partition": [record]}
                self.service._stopping = True
                return {}

            @staticmethod
            def assert_nonblocking(timeout_ms: int) -> None:
                if timeout_ms != 0:
                    raise AssertionError(f"expected timeout_ms=0, got {timeout_ms}")

        consumer = EmptyAfterRecordConsumer()
        service = make_service(batch_size=10, idle_poll=0, consumer=consumer)
        consumer.service = service

        service.run()

        self.assertEqual(service.model.batch_lengths, [1])
        self.assertEqual(committed_offset(service.consumer.commits[0]), 4)

    def test_graceful_shutdown_flushes_partial_batch(self) -> None:
        snapshot = build_snapshot(
            feature_row(0),
            {"connection_id": {"src_ip": "192.0.2.1"}, "timestamp": 1},
        )
        record = kafka_record(0, value=json_dumps_bytes(snapshot))

        class StopAfterPollConsumer(FakeConsumer):
            service: Any

            def poll(self, timeout_ms: int) -> dict[str, list[Any]]:
                self.service._stopping = True
                return {"partition": [record]}

        consumer = StopAfterPollConsumer()
        service = make_service(batch_size=10, idle_poll=0, consumer=consumer)
        consumer.service = service

        service.run()

        self.assertEqual(service.model.batch_lengths, [1])
        self.assertEqual(len(service.producer.sent), 1)
        self.assertEqual(committed_offset(consumer.commits[0]), 1)
        self.assertTrue(consumer.closed)
        self.assertTrue(service.producer.flushed)
        self.assertTrue(service.producer.closed)
        self.assertTrue(service.session.closed)


if __name__ == "__main__":
    unittest.main()
