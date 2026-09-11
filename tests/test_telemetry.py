from __future__ import annotations

import csv
import json
import tempfile
import unittest
from pathlib import Path
from types import ModuleType

from tests.helpers import kafka_import_context, load_module, temporary_modules


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
                "tc35_telemetry_window_tests",
                "intelligent-telemetry/files/telemetry.py",
            )


telemetry = load_telemetry_module()


def metadata(
    *,
    event_at: float,
    label: str,
    connection: int,
    flow_bytes: float,
    flow_pkts: float,
    confidence: float,
    nested: bool = True,
) -> dict[str, object]:
    connection_id = {
        "src_ip": f"192.0.2.{connection}",
        "dst_ip": f"198.51.100.{connection}",
        "src_port": 40_000 + connection,
        "dst_port": 443,
        "protocol": 6,
        "first": connection,
    }
    item: dict[str, object] = {
        "timestamp": event_at,
        "timestamp_nfstream": 1_000.0,
        "timestamp_inference": 1_000.1,
        "timestamp_detector": 1_000.2,
        "monitored_device": "ceos2",
        "interface": "eth3",
        "flow_bytes": flow_bytes,
        "flow_pkts": flow_pkts,
        "ml_confidence": confidence,
        "label": label,
    }
    if nested:
        item["connection_id"] = connection_id
    else:
        item.update(connection_id)
    return item


def event(item: dict[str, object], *, observed_at: float = 1_000.3):
    label = str(item["label"])
    payload = {"schema_version": "v1", "data": [label], "metadata": [item]}
    return telemetry.validated_events(payload, observed_at=observed_at)[0]


def measurement(records, name: str):
    return [record for record in records if record["measurement"] == name]


class TelemetryWindowTests(unittest.TestCase):
    def test_nested_and_flat_connection_metadata_are_equivalent(self) -> None:
        nested = event(
            metadata(
                event_at=100,
                label="normal_traffic",
                connection=1,
                flow_bytes=10,
                flow_pkts=1,
                confidence=0.9,
                nested=True,
            )
        )
        flat = event(
            metadata(
                event_at=100,
                label="normal_traffic",
                connection=1,
                flow_bytes=10,
                flow_pkts=1,
                confidence=0.9,
                nested=False,
            )
        )
        self.assertEqual(nested["connection"], flat["connection"])
        self.assertEqual(nested["src_ip"], "192.0.2.1")
        self.assertEqual(nested["dst_ip"], "198.51.100.1")

    def test_window_calculates_experiment_metrics(self) -> None:
        aggregator = telemetry.EventWindowAggregator(window_seconds=5)
        items = [
            metadata(
                event_at=100,
                label="normal_traffic",
                connection=1,
                flow_bytes=100,
                flow_pkts=10,
                confidence=1.0,
            ),
            metadata(
                event_at=101,
                label="normal_traffic",
                connection=1,
                flow_bytes=150,
                flow_pkts=15,
                confidence=0.0,
            ),
            metadata(
                event_at=102,
                label="normal_traffic",
                connection=2,
                flow_bytes=40,
                flow_pkts=4,
                confidence=1.0,
            ),
            metadata(
                event_at=103,
                label="benign_heavy_hitter",
                connection=3,
                flow_bytes=200,
                flow_pkts=20,
                confidence=0.8,
            ),
            metadata(
                event_at=104,
                label="malign_heavy_hitter",
                connection=4,
                flow_bytes=300,
                flow_pkts=30,
                confidence=0.9,
            ),
        ]
        for item in items:
            completed, disposition = aggregator.add_event(event(item))
            self.assertEqual(completed, [])
            self.assertTrue(disposition["accepted"])

        records = aggregator.flush()
        traffic = {
            record["tags"]["label"]: record["fields"]
            for record in measurement(records, "traffic_window")
        }
        self.assertEqual(traffic["normal_traffic"]["bytes"], 190)
        self.assertEqual(traffic["normal_traffic"]["packets"], 19)
        self.assertEqual(traffic["normal_traffic"]["snapshots"], 3)
        # Connection 1 mean is .5 and connection 2 mean is 1.0.
        self.assertAlmostEqual(traffic["normal_traffic"]["mean_confidence"], 0.75)

        security = measurement(records, "security_window")[0]["fields"]
        self.assertEqual(security["snapshots"], 5)
        self.assertEqual(security["accepted_snapshots"], 5)
        self.assertEqual(security["late_snapshots"], 0)
        self.assertEqual(security["rejected_snapshots"], 0)
        self.assertAlmostEqual(security["security_status"], 0.2)
        self.assertEqual(security["unique_hh_connections"], 1)
        self.assertEqual(security["unique_hh_sources"], 1)
        self.assertEqual(security["unique_hh_targets"], 1)
        self.assertEqual(security["unique_attack_connections"], 1)
        self.assertEqual(security["unique_ddos_attackers"], 1)
        self.assertEqual(security["unique_ddos_targets"], 1)

    def test_cumulative_flow_deltas_ignore_duplicate_and_stale_snapshots(self) -> None:
        tracker = telemetry.FlowDeltaTracker()
        connection = ("a", "b", "1", "2", "6", "first")
        self.assertEqual(tracker.observe(connection, 100, 10), (100, 10))
        self.assertEqual(tracker.observe(connection, 150, 15), (50, 5))
        self.assertEqual(tracker.observe(connection, 150, 15), (0.0, 0.0))
        self.assertEqual(tracker.observe(connection, 125, 12), (0.0, 0.0))

    def test_flow_identity_is_strict_but_historical_unique_count_is_four_tuple(self) -> None:
        aggregator = telemetry.EventWindowAggregator(window_seconds=5)
        first = metadata(
            event_at=100,
            label="malign_heavy_hitter",
            connection=1,
            flow_bytes=100,
            flow_pkts=10,
            confidence=0.9,
        )
        second = metadata(
            event_at=101,
            label="malign_heavy_hitter",
            connection=1,
            flow_bytes=50,
            flow_pkts=5,
            confidence=0.7,
        )
        second_connection = second["connection_id"]
        assert isinstance(second_connection, dict)
        second_connection["protocol"] = 17
        second_connection["first"] = 999
        aggregator.add_event(event(first))
        aggregator.add_event(event(second))
        records = aggregator.flush()
        traffic = {
            record["tags"]["label"]: record["fields"]
            for record in measurement(records, "traffic_window")
        }
        self.assertEqual(traffic["malign_heavy_hitter"]["bytes"], 150)
        security = measurement(records, "security_window")[0]["fields"]
        self.assertEqual(security["unique_attack_connections"], 1)

    def test_previous_window_is_late_and_older_snapshot_is_rejected(self) -> None:
        aggregator = telemetry.EventWindowAggregator(
            window_seconds=5,
            allowed_lateness_seconds=5,
        )
        first = metadata(
            event_at=100,
            label="normal_traffic",
            connection=1,
            flow_bytes=10,
            flow_pkts=1,
            confidence=0.8,
        )
        future = metadata(
            event_at=106,
            label="normal_traffic",
            connection=2,
            flow_bytes=20,
            flow_pkts=2,
            confidence=0.8,
        )
        late = metadata(
            event_at=103,
            label="normal_traffic",
            connection=3,
            flow_bytes=30,
            flow_pkts=3,
            confidence=0.8,
        )
        rejected = metadata(
            event_at=99,
            label="normal_traffic",
            connection=4,
            flow_bytes=40,
            flow_pkts=4,
            confidence=0.8,
        )
        watermark_trigger = metadata(
            event_at=111,
            label="normal_traffic",
            connection=5,
            flow_bytes=50,
            flow_pkts=5,
            confidence=0.8,
        )

        aggregator.add_event(event(first))
        completed, _ = aggregator.add_event(event(future))
        self.assertEqual(completed, [])
        completed, _ = aggregator.add_event(event(watermark_trigger))
        self.assertEqual(measurement(completed, "security_window")[0]["fields"]["snapshots"], 1)
        _, late_disposition = aggregator.add_event(event(late))
        _, rejected_disposition = aggregator.add_event(event(rejected))
        self.assertTrue(late_disposition["accepted"])
        self.assertTrue(late_disposition["late"])
        self.assertTrue(rejected_disposition["rejected"])

        security_records = measurement(aggregator.flush(), "security_window")
        security = next(
            record["fields"]
            for record in security_records
            if record["fields"]["window_start"] == 105
        )
        self.assertEqual(security["snapshots"], 3)
        self.assertEqual(security["accepted_snapshots"], 2)
        self.assertEqual(security["late_snapshots"], 2)
        self.assertEqual(security["rejected_snapshots"], 1)

    def test_future_event_waits_for_watermark_and_preserves_delta_order(self) -> None:
        aggregator = telemetry.EventWindowAggregator(
            window_seconds=5,
            allowed_lateness_seconds=5,
        )
        snapshots = [
            metadata(
                event_at=100,
                label="normal_traffic",
                connection=1,
                flow_bytes=100,
                flow_pkts=10,
                confidence=0.8,
            ),
            metadata(
                event_at=106,
                label="normal_traffic",
                connection=1,
                flow_bytes=160,
                flow_pkts=16,
                confidence=0.8,
            ),
            metadata(
                event_at=104,
                label="normal_traffic",
                connection=1,
                flow_bytes=140,
                flow_pkts=14,
                confidence=0.8,
            ),
            metadata(
                event_at=111,
                label="normal_traffic",
                connection=1,
                flow_bytes=200,
                flow_pkts=20,
                confidence=0.8,
            ),
        ]
        aggregator.add_event(event(snapshots[0]))
        completed, _ = aggregator.add_event(event(snapshots[1]))
        self.assertEqual(completed, [])
        aggregator.add_event(event(snapshots[2]))
        completed, _ = aggregator.add_event(event(snapshots[3]))
        first_window = next(
            record
            for record in measurement(completed, "traffic_window")
            if record["tags"]["label"] == "normal_traffic"
        )
        self.assertEqual(first_window["fields"]["bytes"], 140)
        self.assertEqual(first_window["fields"]["packets"], 14)

    def test_missing_capture_timestamp_is_rejected_for_event_windows(self) -> None:
        item = metadata(
            event_at=100,
            label="normal_traffic",
            connection=1,
            flow_bytes=10,
            flow_pkts=1,
            confidence=0.8,
        )
        del item["timestamp"]
        payload = {"schema_version": "v1", "data": ["normal_traffic"], "metadata": [item]}
        with self.assertRaisesRegex(telemetry.TelemetryError, "timestamp must be numeric"):
            telemetry.validated_events(payload, observed_at=100.3)

    def test_prediction_points_in_one_detector_batch_have_unique_timestamps(self) -> None:
        items = [
            metadata(
                event_at=100 + index,
                label="normal_traffic",
                connection=index + 1,
                flow_bytes=10,
                flow_pkts=1,
                confidence=0.8,
            )
            for index in range(2)
        ]
        payload = {
            "schema_version": "v1",
            "data": ["normal_traffic", "normal_traffic"],
            "metadata": items,
        }
        records = telemetry.measurement_records(payload, observed_at=1_000.3)
        self.assertEqual(records[1]["timestamp_ns"], records[0]["timestamp_ns"] + 1)

    def test_idle_windows_emit_zero_heartbeat_after_first_event(self) -> None:
        aggregator = telemetry.EventWindowAggregator(window_seconds=5)
        item = metadata(
            event_at=100,
            label="normal_traffic",
            connection=1,
            flow_bytes=10,
            flow_pkts=1,
            confidence=0.8,
        )
        aggregator.add_event(event(item, observed_at=1_000.3), observed_at=1_000.3)
        first = aggregator.flush_idle(observed_at=1_005.4)
        self.assertEqual(measurement(first, "security_window")[0]["fields"]["snapshots"], 1)
        heartbeat = aggregator.flush_idle(observed_at=1_010.4)
        security = measurement(heartbeat, "security_window")[0]["fields"]
        self.assertEqual(security["snapshots"], 0)
        for record in measurement(heartbeat, "traffic_window"):
            self.assertEqual(record["fields"]["bytes"], 0)
            self.assertEqual(record["fields"]["packets"], 0)

    def test_event_time_is_rebased_from_first_observation_without_hardcoding(self) -> None:
        aggregator = telemetry.EventWindowAggregator(
            window_seconds=5,
            rebase_event_time=True,
        )
        item = metadata(
            event_at=1_720_708_093.927968,
            label="any_future_label",
            connection=1,
            flow_bytes=10,
            flow_pkts=1,
            confidence=0.5,
        )
        aggregator.add_event(event(item, observed_at=2_000.0), observed_at=2_000.0)
        records = aggregator.flush()
        self.assertEqual(records[0]["timestamp_ns"], 2_005_000_000_000)
        traffic = {
            record["tags"]["label"]: record["fields"]
            for record in measurement(records, "traffic_window")
        }
        self.assertIn("any_future_label", traffic)
        for absent_label in (
            "normal_traffic",
            "benign_heavy_hitter",
            "malign_heavy_hitter",
        ):
            self.assertEqual(traffic[absent_label]["bytes"], 0)
            self.assertEqual(traffic[absent_label]["packets"], 0)
            self.assertEqual(traffic[absent_label]["mean_confidence"], 0)

    def test_optional_csv_exports_include_interval_and_latency_rows(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            interval_path = Path(directory) / "intervals.csv"
            latency_path = Path(directory) / "latencies.csv"
            exports = telemetry.CsvExports(
                interval_path=interval_path,
                latency_path=latency_path,
            )
            aggregator = telemetry.EventWindowAggregator(window_seconds=5)
            current_event = event(
                metadata(
                    event_at=100,
                    label="normal_traffic",
                    connection=1,
                    flow_bytes=10,
                    flow_pkts=1,
                    confidence=0.8,
                ),
                observed_at=1_000.3,
            )
            _, disposition = aggregator.add_event(
                current_event,
                observed_at=1_000.3,
            )
            exports.write_latency(current_event, disposition)
            exports.write_intervals(aggregator.flush())
            exports.flush()

            with interval_path.open(newline="", encoding="utf-8") as handle:
                interval_rows = list(csv.reader(handle))
            with latency_path.open(newline="", encoding="utf-8") as handle:
                latency_rows = list(csv.reader(handle))
            self.assertEqual(interval_rows[0], list(telemetry.CsvExports.INTERVAL_HEADER))
            self.assertEqual(latency_rows[0], list(telemetry.CsvExports.LATENCY_HEADER))
            self.assertEqual(len(interval_rows), 2)
            self.assertEqual(len(latency_rows), 2)
            self.assertEqual(interval_rows[1][:4], ["0", "0", "1", "1"])
            exports.close()

    def test_prediction_measurement_keeps_all_pipeline_latencies(self) -> None:
        item = metadata(
            event_at=100,
            label="normal_traffic",
            connection=1,
            flow_bytes=10,
            flow_pkts=1,
            confidence=0.8,
        )
        item["timestamp_nfstream"] = 100.0
        item["timestamp_inference"] = 100.1
        item["timestamp_detector"] = 100.2
        payload = {"schema_version": "v1", "data": ["normal_traffic"], "metadata": [item]}
        record = telemetry.measurement_records(payload, observed_at=100.3)[0]
        self.assertAlmostEqual(record["fields"]["inference_latency_ms"], 100.0)
        self.assertAlmostEqual(record["fields"]["detection_latency_ms"], 100.0)
        self.assertAlmostEqual(record["fields"]["monitoring_latency_ms"], 100.0)
        self.assertAlmostEqual(record["fields"]["total_pipeline_latency_ms"], 300.0)

    def test_dashboard_exposes_restored_experiment_metrics(self) -> None:
        dashboard_path = (
            Path(__file__).resolve().parents[1]
            / "grafana"
            / "files"
            / "dashboards"
            / "tc35-monitoring.json"
        )
        dashboard = json.loads(dashboard_path.read_text(encoding="utf-8"))
        queries = "\n".join(
            target["query"]
            for panel in dashboard["panels"]
            for target in panel.get("targets", [])
        )
        for metric in (
            "bytes",
            "packets",
            "mean_confidence",
            "security_status",
            "unique_ddos_attackers",
            "unique_hh_connections",
            "late_snapshots",
            "rejected_snapshots",
            "total_pipeline_latency_ms",
        ):
            self.assertIn(metric, queries)
        security_panel = next(
            panel
            for panel in dashboard["panels"]
            if panel["title"].startswith("Security status")
        )
        self.assertNotIn("fn: mean", security_panel["targets"][0]["query"])

    def test_window_environment_preserves_legacy_alias_with_modern_precedence(self) -> None:
        self.assertEqual(
            telemetry.window_seconds_from_environment({"TIME_INTERVAL": "10"}),
            10,
        )
        self.assertEqual(
            telemetry.window_seconds_from_environment(
                {
                    "TIME_INTERVAL": "10",
                    "MONITORING_WINDOW_SECONDS": "2.5",
                }
            ),
            2.5,
        )


if __name__ == "__main__":
    unittest.main()
