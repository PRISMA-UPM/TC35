"""
------------------------------------------------------------------------------------------------------------------------
meter.py
Copyright (C) 2019-22 - NFStream Developers
This file is part of NFStream, a Flexible Network Data Analysis Framework (https://www.nfstream.org/).
NFStream is free software: you can redistribute it and/or modify it under the terms of the GNU Lesser General Public
License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any later
version.
NFStream is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public License for more details.
You should have received a copy of the GNU Lesser General Public License along with NFStream.
If not, see <http://www.gnu.org/licenses/>.
------------------------------------------------------------------------------------------------------------------------
"""

# SPDX-License-Identifier: LGPL-3.0-or-later
# Modified for TC35 (2024-10-07; integration changes 2026-08-01).

from .engine import create_engine, setup_capture, setup_dissector, activate_capture
from .utils import set_affinity, InternalError, NFEvent, NFMode
from collections import OrderedDict
from .flow import NFlow
from .kafka import (
    build_meter_producer,
    cleanup_flush_flow_interval,
    flush_packet_interval,
)


ENGINE_LOAD_ERR = "Error when loading engine library. This means that you are probably building nfstream from source \
and something went wrong during the engine compilation step. Please see: \
https://www.nfstream.org/docs/#building-nfstream-from-sourcesfor more information"

NPCAP_LOAD_ERR = "Error finding npcap library. Please make sure you npcap is installed on your system."

NDPI_LOAD_ERR = "Error while loading Dissector. This means that you are building nfstream with an out of sync nDPI."

FLOW_KEY = "{}:{}:{}:{}:{}:{}:{}:{}:{}"


class NFCache(OrderedDict):
    """ Least recently updated dictionary

    A Cache provides fast and efficient way of retrieving data.
    The NFCache object is used to cache flow records such that least
    recently accessed flow entries are kept on the top(end) and and least
    used will be at the bottom. This way, it will be faster and efficient
    to update flow records.

    """
    def __init__(self, *args, **kwds):
        super().__init__(*args, **kwds)

    def __getitem__(self, key):
        value = super().__getitem__(key)
        self.move_to_end(key)
        return value

    def __setitem__(self, key, value):
        super().__setitem__(key, value)
        self.move_to_end(key)  # now this item is the most recently updated

    def __eq__(self, other):
        return super().__eq__(other)

    def get_lru_key(self):
        return next(iter(self))

    def get_lru_item(self):
        """Return the LRU item without treating the scan as flow activity."""
        key = self.get_lru_key()
        return key, OrderedDict.__getitem__(self, key)


def meter_scan(meter_tick, cache, idle_timeout, channel, udps, sync, n_dissections, statistics, splt, ffi, lib,
               dissector, producer):
    """Checks flow cache for expired flow.

    Expired flows are identified, added to channel and then removed from the cache.
    """
    remaining = True  # We suppose that there is something to expire
    scanned = 0
    while remaining and scanned < 1000:  # idle scan budget (each 10ms we scan 1000 as maximum)
        try:
            flow_key, flow = cache.get_lru_item()
            if flow.is_idle(meter_tick, idle_timeout):  # idle, expire it.
                channel.put(flow.expire(udps, sync, n_dissections, statistics, splt, ffi, lib, dissector, producer, False))
                del cache[flow_key]
                del flow
                scanned += 1
            else:
                remaining = False  # LRU flow is not yet idle.
        except StopIteration:  # Empty cache
            remaining = False
    return scanned


def get_flow_key(src_ip, src_port, dst_ip, dst_port, protocol, vlan_id, tunnel_id):
    """ Create a consistent direction agnostic flow key """
    if src_ip[1] < dst_ip[1] or ((src_ip[1] == dst_ip[1]) and (src_ip[0] < dst_ip[0])):
        key = (src_ip[0], src_ip[1], src_port,
               dst_ip[0], dst_ip[1], dst_port,
               protocol, vlan_id, tunnel_id)
    else:
        if src_ip[0] == dst_ip[0] and src_ip[1] == dst_ip[1]:
            if src_port <= dst_port:
                key = (src_ip[0], src_ip[1], src_port,
                       dst_ip[0], dst_ip[1], dst_port,
                       protocol, vlan_id, tunnel_id)
            else:
                key = (dst_ip[0], dst_ip[1], dst_port,
                       src_ip[0], src_ip[1], src_port,
                       protocol, vlan_id, tunnel_id)
        else:
            key = (dst_ip[0], dst_ip[1], dst_port,
                   src_ip[0], src_ip[1], src_port,
                   protocol, vlan_id, tunnel_id)
    return key


def get_flow_key_from_pkt(packet):
    """ Create flow key from packet information (7-tuple)

    A flow key uniquely determines a flow using source ip,
    destination ip, source port, destination port, TCP/UDP protocol, VLAN ID
    and tunnel ID of the packets.
    """
    return get_flow_key(packet.src_ip,
                        packet.src_port,
                        packet.dst_ip,
                        packet.dst_port,
                        packet.protocol,
                        packet.vlan_id,
                        packet.tunnel_id)


def consume(packet, cache, active_timeout, idle_timeout, channel, ffi, lib, udps, sync, accounting_mode, n_dissections,
            statistics, splt, dissector, decode_tunnels, system_visibility_mode, producer, flush):
    """ consume a packet and produce flow """
    # We maintain state for active flows computation 1 for creation, 0 for update/cut, -1 for custom expire
    flow_key = get_flow_key_from_pkt(packet)
    try:  # update flow
        # quitar si no debug
        #if flush:
        #    print ("flush en consume cuando llamas a flow.update")
        flow = cache[flow_key].update(packet, idle_timeout, active_timeout, ffi, lib, udps, sync, accounting_mode,
                                      n_dissections, statistics, splt, dissector, producer, flush)
        if flow is not None:
            if flow.expiration_id < 0:  # custom expiration
                channel.put(flow)
                del cache[flow_key]
                del flow
                state = -1
            else:  # active/inactive expiration
                channel.put(flow)
                del cache[flow_key]
                del flow
                try:
                    cache[flow_key] = NFlow(packet, ffi, lib, udps, sync, accounting_mode, n_dissections,
                                            statistics, splt, dissector, decode_tunnels, system_visibility_mode)
                except OSError:
                    print("WARNING: Failed to allocate memory space for flow creation. Flow creation aborted.")
                state = 0
        else:
            state = 0
    except KeyError:  # create flow
        try:
            if sync:
                flow = NFlow(packet, ffi, lib, udps, sync, accounting_mode, n_dissections, statistics, splt, dissector,
                             decode_tunnels, system_visibility_mode)
                if flow.expiration_id == -1:  # A user Plugin forced expiration on the first packet
                    channel.put(flow.expire(udps, sync, n_dissections, statistics, splt, ffi, lib, dissector, producer, flush))
                    del flow
                    state = 0
                else:
                    cache[flow_key] = flow
                    state = 1
            else:
                cache[flow_key] = NFlow(packet, ffi, lib, udps, sync, accounting_mode, n_dissections, statistics, splt,
                                        dissector, decode_tunnels, system_visibility_mode)
                state = 1
        except OSError:
            print("WARNING: Failed to allocate memory space for flow creation. Flow creation aborted.")
            state = 0
    return state


def meter_cleanup(cache, channel, udps, sync, n_dissections, statistics, splt, ffi, lib, dissector, producer):
    """ cleanup all entries in NFCache """
    flow_keys = list(cache.keys())
    flush_interval = cleanup_flush_flow_interval()
    for counter, flow_key in enumerate(flow_keys, start=1):
        flush = counter % flush_interval == 0 or counter == len(flow_keys)
        flow = cache[flow_key]
        channel.put(flow.expire(udps, sync, n_dissections, statistics, splt, ffi, lib, dissector, producer, flush))
        if flush:
            flush_meter_producer(producer, udps)
        del cache[flow_key]
        del flow

def capture_track(lib, capture, mode, interface_stats, tracker, processed, ignored):
    """ Update shared performance values """
    lib.capture_stats(capture, interface_stats, mode)
    tracker[0].value = interface_stats.dropped
    tracker[1].value = processed
    tracker[2].value = ignored


def send_error(root_idx, channel, msg):
    channel.put(InternalError(NFEvent.ERROR, f"meter {root_idx}: {msg}"))


def flush_meter_producer(producer, udps):
    """Flush Kafka once, then let plugins surface delivery failures."""
    if producer is None:
        return
    producer.flush(timeout=30)
    for udp in udps:
        callback = getattr(udp, "on_meter_flush", None)
        if callback is not None:
            callback(producer)


def close_meter_producer(producer, udps):
    """Flush and close a meter-owned Kafka producer."""
    try:
        flush_meter_producer(producer, udps)
    finally:
        try:
            producer.close(timeout=30)
        except TypeError:  # Compatibility with simple test doubles.
            producer.close()


def meter_workflow(source, snaplen, decode_tunnels, bpf_filter, promisc, n_roots, root_idx, mode,
                   idle_timeout, active_timeout, accounting_mode, udps, n_dissections, statistics, splt,
                   channel, tracker, lock, group_id, system_visibility_mode, kafka_url,
                   allowed_cpus=None):
    """Metering workflow with one producer per TC35-aware meter."""
    producer = None
    ffi = None
    lib = None
    dissector = None
    barrier_released = False
    failure = None
    failure_message = None
    try:
        flush_rate = flush_packet_interval()
        set_affinity(root_idx + 1, allowed_cpus)
        ffi, lib = create_engine()
        if lib is None:
            raise RuntimeError(ENGINE_LOAD_ERR)

        meter_tick, meter_scan_tick, meter_track_tick = 0, 0, 0
        meter_scan_interval, meter_track_interval = 10, 1000
        cache = NFCache()
        dissector = setup_dissector(ffi, lib, n_dissections)
        if dissector == ffi.NULL and n_dissections:
            raise RuntimeError(NDPI_LOAD_ERR)

        active_flows, ignored_packets, processed_packets = 0, 0, 0
        sync = len(udps) > 0
        interface_stats = ffi.new("struct nf_stat *")

        if any(getattr(udp, "nfmod_meter_context", False) for udp in udps):
            producer = build_meter_producer(kafka_url, root_idx)

        # Ensure that all meter processes start consuming at the same time.
        if root_idx == n_roots - 1:
            lock.release()
        else:
            lock.acquire()
            lock.release()
        barrier_released = True

        sources = source if mode == NFMode.MULTIPLE_FILES else [source]
        for capture_source in sources:
            error_child = ffi.new("char[256]")
            capture = None
            try:
                capture = setup_capture(
                    ffi, lib, capture_source, snaplen, promisc, mode,
                    error_child, group_id,
                )
                if capture is None:
                    raise RuntimeError(
                        ffi.string(error_child).decode("utf-8", errors="ignore")
                    )
                if not activate_capture(capture, lib, error_child, bpf_filter, mode):
                    raise RuntimeError(
                        ffi.string(error_child).decode("utf-8", errors="ignore")
                    )

                remaining_packets = True
                while remaining_packets:
                    nf_packet = ffi.new("struct nf_packet *")
                    ret = lib.capture_next(
                        capture, nf_packet, decode_tunnels, n_roots, root_idx, int(mode)
                    )
                    if ret > 0:  # Valid packet or time ticker.
                        packet_time = nf_packet.time
                        if packet_time > meter_tick:
                            meter_tick = packet_time
                        else:
                            nf_packet.time = meter_tick  # Enforce monotonic flow time.

                        if ret == 1:  # Packet must be processed.
                            processed_packets += 1
                            flush = processed_packets % flush_rate == 0
                            go_scan = False
                            if meter_tick - meter_scan_tick >= meter_scan_interval:
                                go_scan = True
                                meter_scan_tick = meter_tick
                            diff = consume(
                                nf_packet, cache, active_timeout, idle_timeout, channel,
                                ffi, lib, udps, sync, accounting_mode, n_dissections,
                                statistics, splt, dissector, decode_tunnels,
                                system_visibility_mode, producer, flush,
                            )
                            if flush:
                                flush_meter_producer(producer, udps)
                            active_flows += diff
                            if go_scan:
                                idles = meter_scan(
                                    meter_tick, cache, idle_timeout, channel, udps,
                                    sync, n_dissections, statistics, splt, ffi, lib,
                                    dissector, producer,
                                )
                                active_flows -= idles
                        elif meter_tick - meter_scan_tick >= meter_scan_interval:
                            idles = meter_scan(
                                meter_tick, cache, idle_timeout, channel, udps, sync,
                                n_dissections, statistics, splt, ffi, lib, dissector,
                                producer,
                            )
                            active_flows -= idles
                            meter_scan_tick = meter_tick
                    elif ret == 0:
                        ignored_packets += 1
                    elif ret < -1:
                        remaining_packets = False

                    if meter_tick - meter_track_tick >= meter_track_interval:
                        capture_track(
                            lib, capture, mode, interface_stats, tracker,
                            processed_packets, ignored_packets,
                        )
                        meter_track_tick = meter_tick
            finally:
                if capture is not None:
                    lib.capture_close(capture)

        meter_cleanup(
            cache, channel, udps, sync, n_dissections, statistics, splt,
            ffi, lib, dissector, producer,
        )
        print(f"Process ended. Processed packets: {processed_packets}")
    except Exception as error:
        failure = error
        failure_message = f"runtime failure ({type(error).__name__}): {error}"
    finally:
        if not barrier_released and root_idx == n_roots - 1:
            try:
                lock.release()
            except (OSError, ValueError):
                pass
        cleanup_failure = None
        cleanup_messages = []
        try:
            if lib is not None and dissector is not None:
                lib.dissector_cleanup(dissector)
        except Exception as error:
            cleanup_failure = error
            cleanup_messages.append(
                f"dissector cleanup failure ({type(error).__name__}): {error}"
            )
        try:
            if producer is not None:
                close_meter_producer(producer, udps)
        except Exception as error:
            if cleanup_failure is None:
                cleanup_failure = error
            cleanup_messages.append(
                f"producer cleanup failure ({type(error).__name__}): {error}"
            )
        if cleanup_failure is not None:
            cleanup_message = "; ".join(cleanup_messages)
            if failure is None:
                failure = cleanup_failure
                failure_message = cleanup_message
            else:
                failure_message = f"{failure_message}; {cleanup_message}"
        try:
            if failure_message is not None:
                send_error(root_idx, channel, failure_message)
        finally:
            channel.put(None)
    if failure is not None:
        raise failure
