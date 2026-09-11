"""Container smoke test for nfmod plus the wheel-provided native engine."""

from pathlib import Path

from nfmod import NFStreamer
from scapy.all import Ether, IP, TCP, Raw, wrpcap


capture_path = Path("/tmp/tc35-nfmod-smoke.pcap")
packets = [
    Ether()
    / IP(src="192.0.2.1", dst="198.51.100.2")
    / TCP(sport=40_000, dport=443, flags="PA")
    / Raw(b"first"),
    Ether()
    / IP(src="198.51.100.2", dst="192.0.2.1")
    / TCP(sport=443, dport=40_000, flags="PA")
    / Raw(b"second"),
]
packets[0].time = 1_700_000_000.0
packets[1].time = 1_700_000_000.1
wrpcap(capture_path, packets)

streamer = NFStreamer(
    source=str(capture_path),
    accounting_mode=3,
    statistical_analysis=False,
    splt_analysis=0,
    n_dissections=0,
    n_meters=2,
)
flows = list(streamer)
if len(flows) != 1:
    raise AssertionError(f"expected one bidirectional flow, got {len(flows)}")
if flows[0].bidirectional_packets != 2:
    raise AssertionError(
        f"expected two packets, got {flows[0].bidirectional_packets}"
    )

print("nfmod native PCAP smoke test passed")
