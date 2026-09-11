from __future__ import annotations

import csv
import json
import tempfile
import unittest
from pathlib import Path

from tests.helpers import kafka_import_context, load_module

with kafka_import_context():
    detector = load_module(
        "tc35_ai_detector_csv_tests",
        "ai-detector/files/ai_detector.py",
    )


def classified_metadata(index: int) -> dict[str, object]:
    return {
        "timestamp": 1_720_000_000.5 + index,
        "src_ip": f"192.0.2.{index + 1}",
        "dst_ip": "198.51.100.10",
        "src_port": 20_000 + index,
        "dst_port": 443,
        "protocol": 6,
        "features": {
            "udps.protocol": 6,
            "src2dst_bytes": 100 + index,
        },
        "label": "ignored-by-audit-writer",
        "ml_confidence": 0.8,
    }


class DetectorCsvTests(unittest.TestCase):
    def test_batched_predictions_are_flushed_as_one_row_per_snapshot(self) -> None:
        inference_payload = {
            "data": [[0.8, 0.15, 0.05], [0.1, 0.2, 0.7]],
        }
        classified_payload = {
            "data": ["normal_traffic", "malign_heavy_hitter"],
            "metadata": [classified_metadata(0), classified_metadata(1)],
        }
        nested_metadata = classified_payload["metadata"][0]
        nested_metadata["connection_id"] = {
            name: nested_metadata.pop(name)
            for name in ("src_ip", "dst_ip", "src_port", "dst_port", "protocol")
        }

        with tempfile.TemporaryDirectory() as directory:
            writer = detector.DetectionCsvWriter(
                directory,
                instance_id="detector / replica #1",
                hostname="pod/name",
            )
            path = writer.path
            writer.write_batch(inference_payload, classified_payload)

            # write_batch flushes, so the complete batch is readable before close().
            self.assertEqual(path.read_text(encoding="utf-8").count("\n"), 3)
            writer.close()
            writer.close()

            self.assertEqual(path.parent, Path(directory))
            self.assertNotIn(" ", path.name)
            self.assertNotIn("#", path.name)
            with path.open(encoding="utf-8", newline="") as csv_file:
                reader = csv.DictReader(csv_file, delimiter=";")
                rows = list(reader)

        self.assertEqual(tuple(reader.fieldnames or ()), detector.AUDIT_CSV_COLUMNS)
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0]["capture_timestamp"], "1720000000.5")
        self.assertEqual(rows[0]["src_ip"], "192.0.2.1")
        self.assertEqual(rows[0]["protocol"], "6")
        self.assertEqual(rows[1]["label"], "malign_heavy_hitter")
        self.assertEqual(
            json.loads(rows[1]["probabilities"]),
            [0.1, 0.2, 0.7],
        )
        self.assertEqual(
            json.loads(rows[1]["features"]),
            {"udps.protocol": 6, "src2dst_bytes": 101},
        )

    def test_missing_five_tuple_field_is_rejected_without_partial_row(self) -> None:
        metadata = classified_metadata(0)
        metadata.pop("protocol")
        with tempfile.TemporaryDirectory() as directory:
            with detector.DetectionCsvWriter(
                directory,
                instance_id="detector",
                hostname="replica",
            ) as writer:
                with self.assertRaisesRegex(
                    detector.AuditCsvError,
                    "protocol",
                ):
                    writer.write_batch(
                        {"data": [[1.0]]},
                        {"data": ["normal"], "metadata": [metadata]},
                    )
                path = writer.path

            with path.open(encoding="utf-8", newline="") as csv_file:
                rows = list(csv.DictReader(csv_file, delimiter=";"))
            self.assertEqual(rows, [])

    def test_mismatched_batch_lengths_are_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            with detector.DetectionCsvWriter(
                directory,
                instance_id="detector",
                hostname="replica",
            ) as writer:
                with self.assertRaisesRegex(
                    detector.AuditCsvError,
                    "lengths do not match",
                ):
                    writer.write_batch(
                        {"data": [[1.0], [1.0]]},
                        {
                            "data": ["normal"],
                            "metadata": [classified_metadata(0)],
                        },
                    )


if __name__ == "__main__":
    unittest.main()
