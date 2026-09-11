from __future__ import annotations

import json
import os
import unittest
from types import ModuleType, SimpleNamespace
from unittest.mock import patch

from tc35_contract import FEATURE_NAMES_V1
from tests.helpers import kafka_import_context, load_module, temporary_modules


def load_collector_module():
    fake_nfmod = ModuleType("nfmod")

    class NFPlugin:
        pass

    class NFStreamer:
        pass

    fake_nfmod.NFPlugin = NFPlugin
    fake_nfmod.NFStreamer = NFStreamer

    fake_scapy = ModuleType("scapy")
    fake_scapy_utils = ModuleType("scapy.utils")

    class PcapReader:
        pass

    fake_scapy_utils.PcapReader = PcapReader
    with kafka_import_context():
        with temporary_modules(
            {
                "nfmod": fake_nfmod,
                "scapy": fake_scapy,
                "scapy.utils": fake_scapy_utils,
            },
        ):
            return load_module(
                "tc35_nfstream_collector",
                "nfstream/files/nfstream_kafka.py",
            )


def load_meter_module():
    package_name = "tc35_nfmod_test"
    fake_package = ModuleType(package_name)
    fake_package.__path__ = []

    fake_engine = ModuleType(f"{package_name}.engine")
    for name in (
        "create_engine",
        "setup_capture",
        "setup_dissector",
        "activate_capture",
    ):
        setattr(fake_engine, name, lambda *args, **kwargs: None)

    fake_utils = ModuleType(f"{package_name}.utils")
    fake_utils.set_affinity = lambda *args, **kwargs: None
    fake_utils.InternalError = lambda event_id, message: SimpleNamespace(
        id=event_id,
        message=message,
    )
    fake_utils.NFEvent = SimpleNamespace(ERROR="error")
    fake_utils.NFMode = SimpleNamespace(MULTIPLE_FILES=2)

    fake_flow = ModuleType(f"{package_name}.flow")
    fake_flow.NFlow = object
    fake_kafka = ModuleType(f"{package_name}.kafka")
    fake_kafka.build_meter_producer = lambda *args, **kwargs: None
    fake_kafka.cleanup_flush_flow_interval = lambda: 300
    fake_kafka.flush_packet_interval = lambda: 900

    with temporary_modules(
        {
            package_name: fake_package,
            f"{package_name}.engine": fake_engine,
            f"{package_name}.utils": fake_utils,
            f"{package_name}.flow": fake_flow,
            f"{package_name}.kafka": fake_kafka,
        },
    ):
        return load_module(
            f"{package_name}.meter",
            "nfstream/nfmod/meter.py",
        )


collector = load_collector_module()
meter = load_meter_module()
with kafka_import_context():
    nfmod_kafka = load_module(
        "tc35_nfmod_kafka",
        "nfstream/nfmod/kafka.py",
    )


class FakeFuture:
    is_done = True

    @staticmethod
    def get(timeout=None):
        return SimpleNamespace(timeout=timeout)


class FakeProducer:
    instances = []

    def __init__(self, **kwargs):
        self.options = kwargs
        self.sent = []
        self.flushed = False
        self.flush_count = 0
        self.closed = False
        self.instances.append(self)

    def send(self, topic, **kwargs):
        self.sent.append((topic, kwargs))
        return FakeFuture()

    def flush(self, timeout=None):
        self.flushed = True
        self.flush_count += 1

    def close(self, timeout=None):
        self.closed = True


class NFStreamCollectorTests(unittest.TestCase):
    def setUp(self) -> None:
        FakeProducer.instances.clear()

    def test_plugin_computes_and_publishes_exact_rev4_snapshot(self) -> None:
        plugin = collector.KafkaSnapshotPlugin(
            interval_seconds=0.5,
            topic="inference_data",
            monitored_device="test-device",
            interface="test.pcap",
            max_pending_sends=2,
        )
        producer = FakeProducer()
        flow = SimpleNamespace(
            udps=SimpleNamespace(),
            protocol=6,
            src_ip="192.0.2.10",
            dst_ip="198.51.100.20",
            src_port=45_000,
            dst_port=443,
            src2dst_first_seen_ms=1_000,
            dst2src_first_seen_ms=0,
            bidirectional_first_seen_ms=1_000,
            src2dst_bytes=64,
            dst2src_bytes=0,
        )

        plugin.on_init(
            SimpleNamespace(time=1_000, payload_size=10, direction=0),
            flow,
        )
        flow.dst2src_first_seen_ms = 1_050
        flow.dst2src_bytes = 96
        plugin.on_update(
            SimpleNamespace(time=1_050, payload_size=12, direction=1),
            flow,
            producer,
            False,
        )
        flow.src2dst_bytes = 128
        plugin.on_update(
            SimpleNamespace(time=1_600, payload_size=8, direction=0),
            flow,
            producer,
            False,
        )

        [(topic, sent)] = producer.sent
        snapshot = sent["value"]
        self.assertEqual(topic, "inference_data")
        self.assertEqual(tuple(snapshot["feature_names"]), FEATURE_NAMES_V1)
        self.assertEqual(snapshot["snapshot_interval_seconds"], 0.5)
        self.assertEqual(
            snapshot["data"]["features"],
            {
                "udps.protocol": 6,
                "udps.src2dst_last": 600,
                "udps.dst2src_last": 0,
                "udps.src2dst_pkts_data": 2,
                "udps.dst2src_pkts_data": 1,
                "src2dst_bytes": 128,
                "dst2src_bytes": 96,
                "udps.diff_dst_src_first": 50,
            },
        )

        plugin.on_expire(flow, producer, True)
        meter.flush_meter_producer(producer, (plugin,))
        self.assertTrue(producer.flushed)
        self.assertEqual(producer.flush_count, 1)

    def test_meter_flushes_once_even_before_a_snapshot_boundary(self) -> None:
        plugin = collector.KafkaSnapshotPlugin(
            interval_seconds=0.5,
            topic="inference_data",
            monitored_device="test-device",
            interface="test.pcap",
        )
        producer = FakeProducer()
        flow = SimpleNamespace(
            udps=SimpleNamespace(),
            protocol=6,
            src2dst_first_seen_ms=1_000,
            dst2src_first_seen_ms=0,
        )
        plugin.on_init(
            SimpleNamespace(time=1_000, payload_size=0, direction=0),
            flow,
        )
        plugin.on_update(
            SimpleNamespace(time=1_100, payload_size=0, direction=0),
            flow,
            producer,
            True,
        )
        self.assertFalse(producer.flushed)
        meter.flush_meter_producer(producer, (plugin,))
        self.assertEqual(producer.flush_count, 1)
        self.assertEqual(producer.sent, [])

    def test_nfmod_meter_producer_keeps_configured_kafka_settings(self) -> None:
        nfmod_kafka.KafkaProducer = FakeProducer
        settings = {
            "PRODUCER_CLIENT_ID": "test-producer",
            "KAFKA_BATCH_SIZE": "32768",
            "KAFKA_LINGER_MS": "10",
            "KAFKA_COMPRESSION_TYPE": "gzip",
            "KAFKA_ACKS": "1",
            "KAFKA_RETRIES": "0",
        }
        with patch.dict(os.environ, settings):
            producer = nfmod_kafka.build_meter_producer("kafka:9092", 3)

        options = producer.options
        self.assertEqual(options["bootstrap_servers"], "kafka:9092")
        self.assertTrue(options["client_id"].startswith("test-producer-meter-3-"))
        self.assertEqual(options["batch_size"], 32768)
        self.assertEqual(options["linger_ms"], 10)
        self.assertEqual(options["compression_type"], "gzip")
        self.assertEqual(options["acks"], 1)
        self.assertIsInstance(options["acks"], int)
        self.assertEqual(options["retries"], 0)
        self.assertEqual(
            json.loads(options["value_serializer"]({"schema_version": "v1"})),
            {"schema_version": "v1"},
        )

        with patch.dict(os.environ, {"KAFKA_ACKS": "invalid"}):
            with self.assertRaisesRegex(ValueError, "KAFKA_ACKS"):
                nfmod_kafka.build_meter_producer("kafka:9092", 3)

    def test_nfmod_cleanup_handles_empty_cache_and_flushes_final_batch(self) -> None:
        class Channel:
            def __init__(self):
                self.items = []

            def put(self, item):
                self.items.append(item)

        class Flow:
            def __init__(self):
                self.flush = None

            def expire(self, *args):
                self.flush = args[-1]
                return self

        empty_channel = Channel()
        meter.meter_cleanup(
            {}, empty_channel, (), True, 0, False, 0,
            None, None, None, FakeProducer(),
        )
        self.assertEqual(empty_channel.items, [])

        flows = [Flow() for _ in range(601)]
        cache = {index: flow for index, flow in enumerate(flows)}
        channel = Channel()
        producer = FakeProducer()
        with patch.object(meter, "cleanup_flush_flow_interval", return_value=300):
            meter.meter_cleanup(
                cache, channel, (), True, 0, False, 0,
                None, None, None, producer,
            )

        self.assertEqual(cache, {})
        self.assertEqual(len(channel.items), 601)
        self.assertEqual(
            [index + 1 for index, flow in enumerate(flows) if flow.flush],
            [300, 600, 601],
        )
        self.assertEqual(producer.flush_count, 3)

    def test_nfmod_lru_scan_expires_newer_idle_flows(self) -> None:
        class Channel:
            def __init__(self):
                self.items = []

            def put(self, item):
                self.items.append(item)

        class Flow:
            def __init__(self, idle):
                self.idle = idle

            def is_idle(self, meter_tick, idle_timeout):
                return self.idle

            def expire(self, *args):
                return self

        active = Flow(idle=False)
        idle = Flow(idle=True)
        cache = meter.NFCache()
        cache["active"] = active
        cache["idle"] = idle

        # A real packet update of the old active flow must move it behind the
        # more recently created but now idle flow.
        self.assertIs(cache["active"], active)
        self.assertEqual(list(cache), ["idle", "active"])
        self.assertEqual(cache.get_lru_item(), ("idle", idle))
        self.assertEqual(list(cache), ["idle", "active"])

        channel = Channel()
        scanned = meter.meter_scan(
            meter_tick=10_000,
            cache=cache,
            idle_timeout=1_000,
            channel=channel,
            udps=(),
            sync=False,
            n_dissections=0,
            statistics=False,
            splt=0,
            ffi=None,
            lib=None,
            dissector=None,
            producer=None,
        )
        self.assertEqual(scanned, 1)
        self.assertEqual(list(cache), ["active"])
        self.assertEqual(channel.items, [idle])

    def test_nfmod_startup_failure_is_reported_and_unblocks_peers(self) -> None:
        class Channel:
            def __init__(self):
                self.items = []

            def put(self, item):
                self.items.append(item)

        class Lock:
            def __init__(self):
                self.releases = 0

            def release(self):
                self.releases += 1

        channel = Channel()
        lock = Lock()
        with patch.object(
            meter,
            "flush_packet_interval",
            side_effect=ValueError("bad flush interval"),
        ):
            with self.assertRaisesRegex(ValueError, "bad flush interval"):
                meter.meter_workflow(
                    source="capture.pcap",
                    snaplen=1536,
                    decode_tunnels=True,
                    bpf_filter=None,
                    promisc=True,
                    n_roots=1,
                    root_idx=0,
                    mode=0,
                    idle_timeout=120_000,
                    active_timeout=1_800_000,
                    accounting_mode=3,
                    udps=(),
                    n_dissections=0,
                    statistics=False,
                    splt=0,
                    channel=channel,
                    tracker=None,
                    lock=lock,
                    group_id=1,
                    system_visibility_mode=0,
                    kafka_url="kafka:9092",
                )

        self.assertEqual(lock.releases, 1)
        self.assertEqual(channel.items[-1], None)
        self.assertEqual(channel.items[0].id, "error")
        self.assertIn("bad flush interval", channel.items[0].message)

    def test_plugin_rejects_wrong_model_interval(self) -> None:
        with self.assertRaisesRegex(ValueError, "requires interval_seconds=0.5"):
            collector.KafkaSnapshotPlugin(
                interval_seconds=1.0,
                topic="inference_data",
                monitored_device="test-device",
                interface="test.pcap",
            )

    def test_pcap_freno_preserves_quarter_second_lead(self) -> None:
        plugin = collector.KafkaSnapshotPlugin(
            interval_seconds=0.5,
            topic="inference_data",
            monitored_device="test-device",
            interface="test.pcap",
            replay_max_lead_seconds=0.25,
            first_capture_timestamp=10.0,
        )
        plugin.wall_clock_start = 100.0

        with (
            patch.object(collector.time, "time", return_value=101.0),
            patch.object(collector.time, "sleep") as sleep,
        ):
            plugin._pace_replay(11_500)

        sleep.assert_called_once_with(0.25)

    def test_realtime_pacing_has_no_lead_and_unpaced_mode_does_not_sleep(self) -> None:
        realtime = collector.KafkaSnapshotPlugin(
            interval_seconds=0.5,
            topic="inference_data",
            monitored_device="test-device",
            interface="test.pcap",
            replay_max_lead_seconds=0.0,
            first_capture_timestamp=10.0,
        )
        realtime.wall_clock_start = 100.0
        unpaced = collector.KafkaSnapshotPlugin(
            interval_seconds=0.5,
            topic="inference_data",
            monitored_device="test-device",
            interface="test.pcap",
        )

        with (
            patch.object(collector.time, "time", return_value=101.0),
            patch.object(collector.time, "sleep") as sleep,
        ):
            realtime._pace_replay(11_500)
            unpaced._pace_replay(11_500)

        sleep.assert_called_once_with(0.5)

    def test_capture_defaults_are_freno_and_six_meters(self) -> None:
        with (
            patch.dict(os.environ, {}, clear=True),
            patch("sys.argv", ["nfstream_kafka.py"]),
        ):
            args = collector.parse_args()

        self.assertEqual(args.capture_mode, "pcap-freno")
        self.assertEqual(args.pcap_freno_max_lead_seconds, 0.25)
        self.assertEqual(args.n_meters, 6)
        self.assertTrue(args.require_exact_meters)


if __name__ == "__main__":
    unittest.main()
