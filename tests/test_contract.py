from __future__ import annotations

import math
import unittest
from types import SimpleNamespace

from tc35_contract import (
    ContractError,
    FEATURE_NAMES_V1,
    build_snapshot,
    extract_snapshot_batch,
    iter_polled_records,
    json_dumps_bytes,
    json_loads_bytes,
)
from tests.helpers import kafka_import_context


def sample_features() -> dict[str, float]:
    return {
        "dst2src_bytes": 120,
        "udps.dst2src_pkts_data": 2,
        "udps.protocol": 6,
        "src2dst_bytes": 300,
        "udps.src2dst_last": 500,
        "udps.dst2src_last": 420,
        "udps.src2dst_pkts_data": 4,
        "udps.diff_dst_src_first": 15,
    }


class MessageContractTests(unittest.TestCase):
    def test_single_snapshot_is_reordered_to_model_schema(self) -> None:
        snapshot = build_snapshot(
            sample_features(),
            {"connection_id": {"src_ip": "192.0.2.1"}, "timestamp": 1},
        )
        rows, metadata, version = extract_snapshot_batch(
            snapshot,
            [
                ("features_names", ",".join(FEATURE_NAMES_V1).encode()),
                ("version", b"v1"),
            ],
        )
        self.assertEqual(tuple(rows[0]), FEATURE_NAMES_V1)
        self.assertEqual(metadata[0]["src_ip"], "192.0.2.1")
        self.assertEqual(version, "v1")

    def test_legacy_batch_form_is_supported(self) -> None:
        snapshot = build_snapshot(sample_features(), {"connection_id": {}})
        rows, metadata, _ = extract_snapshot_batch([snapshot, snapshot], None)
        self.assertEqual(len(rows), 2)
        self.assertEqual(len(metadata), 2)

    def test_missing_extra_and_nonfinite_features_are_rejected(self) -> None:
        missing = sample_features()
        missing.pop("dst2src_bytes")
        with self.assertRaisesRegex(ContractError, "missing"):
            build_snapshot(missing, {})

        extra = sample_features()
        extra["legacy.tstat"] = 1
        with self.assertRaisesRegex(ContractError, "extra"):
            build_snapshot(extra, {})

        nonfinite = sample_features()
        nonfinite["dst2src_bytes"] = math.inf
        with self.assertRaisesRegex(ContractError, "finite"):
            build_snapshot(nonfinite, {})

    def test_json_transport_rejects_nan(self) -> None:
        with self.assertRaises(ValueError):
            json_dumps_bytes({"bad": math.nan})
        with self.assertRaises(ValueError):
            json_loads_bytes(b'{"bad":NaN}')
        self.assertEqual(json_loads_bytes(json_dumps_bytes({"ok": 1})), {"ok": 1})

    def test_snapshot_interval_must_match_rev4_contract(self) -> None:
        snapshot = build_snapshot(sample_features(), {"connection_id": {}})
        snapshot["snapshot_interval_seconds"] = 1.0
        with self.assertRaisesRegex(ContractError, "interval"):
            extract_snapshot_batch(snapshot, None)

    def test_all_kafka_partitions_are_iterated(self) -> None:
        polled = {"partition-0": [1, 2], "partition-1": [3]}
        self.assertEqual(list(iter_polled_records(polled)), [1, 2, 3])

    def test_exact_processed_offset_is_committed(self) -> None:
        with kafka_import_context():
            from tc35_contract.kafka import commit_processed_record

        class Consumer:
            offsets = None

            def commit(self, *, offsets):
                self.offsets = offsets

        consumer = Consumer()
        commit_processed_record(
            consumer,
            SimpleNamespace(topic="events", partition=2, offset=41),
        )
        [(partition, offset)] = consumer.offsets.items()
        self.assertEqual((partition.topic, partition.partition), ("events", 2))
        self.assertEqual(offset.offset, 42)


if __name__ == "__main__":
    unittest.main()
