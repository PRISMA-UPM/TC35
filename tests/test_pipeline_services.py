from __future__ import annotations

import unittest
from types import ModuleType

from tc35_contract import FEATURE_NAMES_V1
from tests.helpers import kafka_import_context, load_module, temporary_modules

with kafka_import_context():
    detector = load_module("tc35_ai_detector", "ai-detector/files/ai_detector.py")
    inference = load_module("tc35_ai_inference", "ai-inference/files/ai_inference.py")


def load_telemetry_module():
    fake_influx = ModuleType("influxdb_client")
    fake_influx.InfluxDBClient = object
    fake_influx.Point = object
    fake_influx.WritePrecision = type("WritePrecision", (), {"NS": "ns"})
    fake_client = ModuleType("influxdb_client.client")
    fake_write_api = ModuleType("influxdb_client.client.write_api")
    fake_write_api.SYNCHRONOUS = object()
    with kafka_import_context():
        with temporary_modules(
            {
                "influxdb_client": fake_influx,
                "influxdb_client.client": fake_client,
                "influxdb_client.client.write_api": fake_write_api,
            },
        ):
            return load_module(
                "tc35_telemetry",
                "intelligent-telemetry/files/telemetry.py",
            )


telemetry = load_telemetry_module()


class FakeModel:
    feature_names_in_ = FEATURE_NAMES_V1
    n_features_in_ = 8
    classes_ = (0, 1, 2)


class PipelineServiceTests(unittest.TestCase):
    def test_inference_requires_exact_nfstream_model_schema(self) -> None:
        names = inference.model_feature_names(
            FakeModel(),
            {"feature_names": list(FEATURE_NAMES_V1)},
        )
        self.assertEqual(names, FEATURE_NAMES_V1)
        self.assertEqual(
            inference.model_snapshot_interval({"snapshot_interval_seconds": 0.5}),
            0.5,
        )
        self.assertEqual(
            inference.model_label_correspondence(
                FakeModel(),
                {
                    "label_correspondence": {
                        "0": "normal_traffic",
                        "1": "benign_heavy_hitter",
                        "2": "malign_heavy_hitter",
                    }
                },
            )["2"],
            "malign_heavy_hitter",
        )

    def test_inference_rejects_probability_class_order_mismatch(self) -> None:
        class WrongClasses(FakeModel):
            classes_ = (0, 2)

        with self.assertRaisesRegex(ValueError, "contiguous"):
            inference.model_label_correspondence(
                WrongClasses(),
                {"label_correspondence": {"0": "normal", "2": "malign"}},
            )

    def test_nfstream_millisecond_timestamp_is_normalized_once(self) -> None:
        metadata = [{"timestamp": 1_720_000_000_500, "src_ip": "192.0.2.1"}]
        rows = [dict.fromkeys(FEATURE_NAMES_V1, 0)]
        prepared = inference.prepare_metadata(metadata, rows, now=123.5)
        self.assertEqual(prepared[0]["timestamp"], 1_720_000_000.5)
        self.assertEqual(prepared[0]["timestamp_inference"], 123.5)
        self.assertEqual(tuple(prepared[0]["features"]), FEATURE_NAMES_V1)

    def test_detector_and_telemetry_contract(self) -> None:
        classified = detector.classify_payload(
            {
                "schema_version": "v1",
                "model": {"id": "model-id"},
                "data": [[0.1, 0.2, 0.7]],
                "metadata": [
                    {
                        "timestamp_nfstream": 100.0,
                        "timestamp_inference": 100.1,
                        "flow_bytes": 42,
                        "flow_pkts": 3,
                    }
                ],
                "label_correspondence": {
                    "0": "normal_traffic",
                    "1": "benign_heavy_hitter",
                    "2": "malign_heavy_hitter",
                },
            },
            now=100.2,
        )
        self.assertEqual(classified["data"], ["malign_heavy_hitter"])
        self.assertEqual(classified["metadata"][0]["ml_confidence"], 0.7)

        records = telemetry.measurement_records(classified)
        self.assertEqual(records[0]["tags"]["label"], "malign_heavy_hitter")
        self.assertAlmostEqual(records[0]["fields"]["inference_latency_ms"], 100.0)
        self.assertAlmostEqual(records[0]["fields"]["detection_latency_ms"], 100.0)
        self.assertIn("src_ip", records[0]["fields"])
        self.assertNotIn("src_ip", records[0]["tags"])

    def test_telemetry_rejects_missing_required_metrics(self) -> None:
        with self.assertRaisesRegex(telemetry.TelemetryError, "timestamp_detector"):
            telemetry.measurement_records(
                {
                    "schema_version": "v1",
                    "data": ["normal_traffic"],
                    "metadata": [{}],
                }
            )

    def test_detector_rejects_probability_metadata_length_mismatch(self) -> None:
        with self.assertRaisesRegex(detector.DetectionError, "metadata length"):
            detector.classify_payload(
                {
                    "schema_version": "v1",
                    "data": [[1.0], [1.0]],
                    "metadata": [{}],
                    "label_correspondence": {"0": "normal"},
                }
            )


if __name__ == "__main__":
    unittest.main()
