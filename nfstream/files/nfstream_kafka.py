"""Publish rev4-compatible NFStream flow snapshots to Kafka as JSON."""

from __future__ import annotations

import argparse
import logging
import math
import os
import random
import time
from collections import deque
from pathlib import Path
from typing import Any

from nfmod import NFPlugin, NFStreamer
from scapy.utils import PcapReader

from tc35_contract import (
    FEATURE_NAMES_V1,
    SCHEMA_VERSION,
    SNAPSHOT_INTERVAL_SECONDS_V1,
    build_snapshot,
)

LOGGER = logging.getLogger("tc35.nfstream")


class KafkaSnapshotPlugin(NFPlugin):
    """Create cumulative 0.5-second flow snapshots for the rev4 model."""

    nfmod_meter_context = True

    def __init__(
        self,
        *,
        interval_seconds: float,
        topic: str,
        monitored_device: str,
        interface: str,
        sampling_rate: float = 1.0,
        replay_max_lead_seconds: float | None = None,
        first_capture_timestamp: float | None = None,
        max_pending_sends: int = 1000,
    ) -> None:
        super().__init__()
        if not math.isclose(
            interval_seconds,
            SNAPSHOT_INTERVAL_SECONDS_V1,
            rel_tol=0.0,
            abs_tol=1e-9,
        ):
            raise ValueError(
                "NFStream v1 requires interval_seconds="
                f"{SNAPSHOT_INTERVAL_SECONDS_V1}"
            )
        if not 0 < sampling_rate <= 1:
            raise ValueError("sampling_rate must be in the range (0, 1]")
        if max_pending_sends < 1:
            raise ValueError("max_pending_sends must be at least one")

        self.interval_seconds = interval_seconds
        self.topic = topic
        self.monitored_device = monitored_device
        self.interface = interface
        self.sampling_rate = sampling_rate
        if replay_max_lead_seconds is not None and replay_max_lead_seconds < 0:
            raise ValueError("replay_max_lead_seconds cannot be negative")
        self.replay_max_lead_seconds = replay_max_lead_seconds
        self.first_capture_timestamp = first_capture_timestamp
        self.wall_clock_start = time.time()
        self.max_pending_sends = max_pending_sends
        self._pending_sends: deque[Any] = deque()
        self.snapshots_sent = 0

    def _reap_pending_sends(self, *, drain: bool = False) -> None:
        while self._pending_sends:
            oldest = self._pending_sends[0]
            must_wait = drain or len(self._pending_sends) >= self.max_pending_sends
            if not oldest.is_done and not must_wait:
                break
            oldest.get(timeout=30 if must_wait else 0)
            self._pending_sends.popleft()

    def on_init(self, packet: Any, flow: Any) -> None:
        flow.udps.start_interval = packet.time
        flow.udps.end_interval = packet.time + self.interval_seconds * 1000
        flow.udps.timestamp = packet.time
        flow.udps.src2dst_last = 0
        flow.udps.dst2src_last = 0
        flow.udps.diff_dst_src_first = self._first_packet_delta(flow)

        is_payload = packet.payload_size > 0
        flow.udps.src2dst_pkts_data = int(is_payload and packet.direction == 0)
        flow.udps.dst2src_pkts_data = int(is_payload and packet.direction == 1)

    @staticmethod
    def _first_packet_delta(flow: Any) -> int | float:
        if flow.dst2src_first_seen_ms and flow.src2dst_first_seen_ms:
            return flow.dst2src_first_seen_ms - flow.src2dst_first_seen_ms
        return 0

    def _pace_replay(self, packet_time_ms: int | float) -> None:
        if (
            self.replay_max_lead_seconds is None
            or self.first_capture_timestamp is None
        ):
            return
        replay_elapsed = packet_time_ms / 1000.0 - self.first_capture_timestamp
        delay = (
            self.wall_clock_start
            + replay_elapsed
            - time.time()
            - self.replay_max_lead_seconds
        )
        if delay > 0:
            time.sleep(delay)

    def _snapshot(self, flow: Any) -> dict[str, Any]:
        features = {
            "udps.protocol": flow.protocol,
            "udps.src2dst_last": flow.udps.src2dst_last,
            "udps.dst2src_last": flow.udps.dst2src_last,
            "udps.src2dst_pkts_data": flow.udps.src2dst_pkts_data,
            "udps.dst2src_pkts_data": flow.udps.dst2src_pkts_data,
            "src2dst_bytes": flow.src2dst_bytes,
            "dst2src_bytes": flow.dst2src_bytes,
            "udps.diff_dst_src_first": flow.udps.diff_dst_src_first,
        }
        metadata = {
            "timestamp": flow.udps.timestamp,
            "timestamp_nfstream": time.time(),
            "timestamp_ini_interval": flow.udps.start_interval,
            "timestamp_fin_interval": flow.udps.end_interval,
            "monitored_device": self.monitored_device,
            "interface": self.interface,
            "connection_id": {
                "src_ip": flow.src_ip,
                "dst_ip": flow.dst_ip,
                "src_port": flow.src_port,
                "dst_port": flow.dst_port,
                "protocol": flow.protocol,
                "first": flow.bidirectional_first_seen_ms,
            },
            "flow_pkts": (
                flow.udps.src2dst_pkts_data + flow.udps.dst2src_pkts_data
            ),
            "flow_bytes": flow.src2dst_bytes + flow.dst2src_bytes,
        }
        return build_snapshot(
            features,
            metadata,
            interval_seconds=self.interval_seconds,
        )

    def on_update(
        self,
        packet: Any,
        flow: Any,
        producer: Any,
        flush: bool,
    ) -> None:
        if packet.payload_size > 0:
            if packet.direction == 0:
                flow.udps.src2dst_last = packet.time - flow.src2dst_first_seen_ms
                flow.udps.src2dst_pkts_data += 1
            else:
                flow.udps.dst2src_last = packet.time - flow.dst2src_first_seen_ms
                flow.udps.dst2src_pkts_data += 1

        if packet.time <= flow.udps.end_interval:
            self._reap_pending_sends()
            return

        flow.udps.diff_dst_src_first = self._first_packet_delta(flow)
        flow.udps.timestamp = packet.time
        self._pace_replay(packet.time)

        if random.random() <= self.sampling_rate:
            snapshot = self._snapshot(flow)
            connection = snapshot["metadata"]["connection_id"]
            key = ",".join(
                str(connection[field])
                for field in ("src_ip", "dst_ip", "src_port", "dst_port", "protocol")
            )
            future = producer.send(
                self.topic,
                key=key,
                value=snapshot,
                headers=[
                    ("version", SCHEMA_VERSION.encode("utf-8")),
                    ("features_names", ",".join(FEATURE_NAMES_V1).encode("utf-8")),
                ],
                timestamp_ms=int(time.time() * 1000),
            )
            self._pending_sends.append(future)
            self._reap_pending_sends()
            self.snapshots_sent += 1
            if self.snapshots_sent % 1000 == 0:
                LOGGER.info("Published %d snapshots from worker", self.snapshots_sent)

        flow.udps.start_interval = packet.time
        flow.udps.end_interval = packet.time + self.interval_seconds * 1000
        self._reap_pending_sends()

    def on_expire(self, flow: Any, producer: Any, flush: bool) -> None:
        self._reap_pending_sends()

    def on_meter_flush(self, producer: Any) -> None:
        """Observe all delivery results after the meter flushes Kafka."""
        self._reap_pending_sends(drain=True)


def first_pcap_timestamp(source: Path) -> float:
    with PcapReader(str(source)) as reader:
        packet = reader.read_packet()
    if packet is None:
        raise ValueError(f"capture is empty: {source}")
    return float(packet.time)


def capture_source(mode: str, pcap_file: str | None, interface: str | None) -> tuple[str, float | None]:
    if mode in {"pcap", "pcap-realtime", "pcap-freno"}:
        if not pcap_file:
            raise ValueError("PCAP_FILE is required for PCAP capture modes")
        source = Path(pcap_file)
        if not source.is_file():
            raise ValueError(f"PCAP file does not exist: {source}")
        if source.stat().st_size == 0:
            raise ValueError(f"PCAP file is empty: {source}")
        first_timestamp = first_pcap_timestamp(source) if mode != "pcap" else None
        return str(source), first_timestamp

    if mode == "network":
        if not interface:
            raise ValueError("INTERFACE is required for live network capture")
        return interface, None
    raise ValueError(f"unsupported CAPTURE_MODE: {mode!r}")


def env_bool(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    normalized = value.strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    raise ValueError(f"{name} must be a boolean")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--capture-mode",
        choices=("pcap", "pcap-realtime", "pcap-freno", "network"),
        default=os.getenv("CAPTURE_MODE", "pcap-freno"),
    )
    parser.add_argument("--pcap-file", default=os.getenv("PCAP_FILE"))
    parser.add_argument("--interface", default=os.getenv("INTERFACE"))
    parser.add_argument(
        "--interval-seconds",
        type=float,
        default=float(os.getenv("T_LIM", "0.5")),
    )
    parser.add_argument("--kafka-url", default=os.getenv("KAFKA_URL", "kafka:9092"))
    parser.add_argument(
        "--producer-topic",
        default=os.getenv("PRODUCER_TOPIC", "inference_data"),
    )
    parser.add_argument(
        "--producer-client-id",
        default=os.getenv("PRODUCER_CLIENT_ID", "nfstream-producer"),
    )
    parser.add_argument(
        "--monitored-device",
        default=os.getenv("MONITORED_DEVICE", "unknown"),
    )
    parser.add_argument(
        "--sampling-rate",
        type=float,
        default=float(os.getenv("SAMPLING_RATE", "1.0")),
    )
    parser.add_argument(
        "--n-meters",
        type=int,
        default=int(os.getenv("NFSTREAM_METERS", "6")),
    )
    parser.add_argument(
        "--require-exact-meters",
        action=argparse.BooleanOptionalAction,
        default=env_bool("NFSTREAM_REQUIRE_EXACT_METERS", True),
        help="fail if NFStream caps the requested meter count to available CPUs",
    )
    parser.add_argument(
        "--idle-timeout",
        type=int,
        default=int(os.getenv("IDLE_TIMEOUT", "120")),
    )
    parser.add_argument(
        "--max-pending-sends",
        type=int,
        default=int(os.getenv("KAFKA_MAX_PENDING_SENDS", "1000")),
    )
    parser.add_argument(
        "--pcap-freno-max-lead-seconds",
        type=float,
        default=float(os.getenv("PCAP_FRENO_MAX_LEAD_SECONDS", "0.25")),
        help=(
            "maximum wall-clock lead allowed in pcap-freno mode; "
            "the experiment configuration uses 0.25 seconds"
        ),
    )
    return parser.parse_args()


def main() -> None:
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s %(levelname)-8s %(name)s: %(message)s",
    )
    args = parse_args()
    # nfmod creates each producer inside its meter process. Export the parsed
    # client-id base so CLI overrides are inherited by those workers.
    os.environ["PRODUCER_CLIENT_ID"] = args.producer_client_id
    source, first_timestamp = capture_source(
        args.capture_mode,
        args.pcap_file,
        args.interface,
    )
    plugin = KafkaSnapshotPlugin(
        interval_seconds=args.interval_seconds,
        topic=args.producer_topic,
        monitored_device=args.monitored_device,
        interface=args.interface or Path(source).name,
        sampling_rate=args.sampling_rate,
        replay_max_lead_seconds=(
            0.0
            if args.capture_mode == "pcap-realtime"
            else args.pcap_freno_max_lead_seconds
            if args.capture_mode == "pcap-freno"
            else None
        ),
        first_capture_timestamp=first_timestamp,
        max_pending_sends=args.max_pending_sends,
    )
    streamer = NFStreamer(
        source=source,
        accounting_mode=3,
        udps=plugin,
        statistical_analysis=False,
        splt_analysis=0,
        idle_timeout=args.idle_timeout,
        n_meters=args.n_meters,
        n_dissections=0,
        kafka_url=args.kafka_url,
    )

    effective_meters = streamer.n_meters
    if effective_meters != args.n_meters:
        message = (
            f"requested {args.n_meters} NFStream meters but only "
            f"{effective_meters} can run with the CPUs visible to the container"
        )
        if args.require_exact_meters:
            raise RuntimeError(message)
        LOGGER.warning(message)

    LOGGER.info(
        "Starting %s capture from %s with %ss snapshots and %d meter(s)",
        args.capture_mode,
        source,
        args.interval_seconds,
        effective_meters,
    )
    flow_count = 0
    try:
        for _flow in streamer:
            flow_count += 1
    except KeyboardInterrupt:
        LOGGER.error("Capture interrupted before completion")
        raise SystemExit(130)
    LOGGER.info("NFStream finished after processing %d flows", flow_count)


if __name__ == "__main__":
    main()
