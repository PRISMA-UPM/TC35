from typing import List
from time import sleep
from kafka import KafkaConsumer
from influxdb_client import WritePrecision, InfluxDBClient, Point, WriteOptions
from influxdb_client.client.write_api import SYNCHRONOUS
from datetime import datetime, timedelta
import logging
import pickle
from collections import namedtuple, defaultdict
import time
import signal
import argparse
from sys import stdout, exit
import requests
import multiprocessing as mp

# Setup LOGGER
LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.INFO)
logFormatter = logging.Formatter(fmt='%(asctime)s.%(msecs)03d %(levelname)-8s %(funcName)20s %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
consoleHandler = logging.StreamHandler(stdout)
consoleHandler.setFormatter(logFormatter)
LOGGER.addHandler(consoleHandler)

ConsumerConfig = namedtuple("ConsumerConfig", ["client_id", "group_id", "topic", "max_poll_records"])
InfluxConfig = namedtuple("InfluxConfig", ["influx_url", "influx_token", "influx_org", "influx_org_id", "influx_bucket", "influx_username", "influx_password"])


class AIMonitoring:
    def __init__(self, kafka_url: str, consumer_client_id: str, consumer_group_id: str, consumer_topic: str, max_poll_records: int,
                 influx_url: str, influx_token: str, influx_org: str, influx_bucket: str, influx_username: str,
                 influx_password: str, time_interval: float):
        signal.signal(signal.SIGINT, self.handler)
        self.monitored_time_interval_agg = float(time_interval)
        self.broker = kafka_url
        self.consumer_config = ConsumerConfig(consumer_client_id, consumer_group_id, consumer_topic, max_poll_records)
        LOGGER.info("Kafka consumer config: %s", self.consumer_config)
        self.consumer = self.connect_to_kafka()
        self.influx_client, self.write_api, influx_org_id = self.connect_to_influxdb(influx_url, influx_token, influx_org,
                                                                      influx_username, influx_password, influx_bucket)
        self.influx_config = InfluxConfig(influx_url, influx_token, influx_org, influx_org_id, influx_bucket, influx_username, influx_password)
        LOGGER.info("InfluxDB config: %s", self.influx_config)
        
        self.label_correspondence = {"LABEL_HH_MALIGN": "malign_heavy_hitter", "LABEL_HH_BENIGN": "benign_heavy_hitter", "LABEL_STANDARD": "normal_traffic"}

        self.monitored_device = "ceos2"
        self.interface = "eth3"
        
        self.records_queue = mp.Queue()
        self.stop_event = mp.Event()
        self.add_records_process = mp.Process(target=self.add_records, args=(self.records_queue, self.stop_event))

    def handler(self, num, frame):
        LOGGER.info("Gracefully stopping...")
        try:
            self.stop_event.set()
            self.add_records_process.join()
            self.consumer.close()
            self.influx_client.close()
            exit()
        except:
            exit()

    def connect_to_influxdb(self, url: str, token: str, org_name: str, username: str, password: str, bucket: str):
        LOGGER.info("Attempting to connect to InfluxDB...")
        try:
            client = InfluxDBClient(url=url, token=token, org=org_name, username=username, password=password, timeout=50000)
            status = client.ping()
            if status:
                write_api = client.write_api(write_options=SYNCHRONOUS)
                organizations_api = client.organizations_api()
                orgs = organizations_api.find_organizations()
                for org in orgs:
                    if org.name == org_name:
                        org_id = org.id
                LOGGER.info("Successfully connected to InfluxDB")
                return client, write_api, org_id
            else:
                LOGGER.error("Could not connect to InfluxDB")
        except Exception as e:
            LOGGER.error("Failed to connect to InfluxDB: %s: %s", type(e).__name__, e)

    def connect_to_kafka(self) -> KafkaConsumer:
        """
        Creates and connects a KafkaConsumer instance to the Kafka broker specified in self.broker
        Returns:
            A KafkaConsumer instance.
        """
        LOGGER.info("Attempting to establish connection to Kafka broker %s", self.broker)

        consumer = KafkaConsumer(self.consumer_config.topic, bootstrap_servers=self.broker,
                                 client_id=self.consumer_config.client_id,
                                 group_id=self.consumer_config.group_id, value_deserializer=pickle.loads, max_poll_records=self.consumer_config.max_poll_records)

        LOGGER.info("Trying to establish connection to brokers...")
        LOGGER.info("Consumer connection status: %s", consumer.bootstrap_connected())

        # Validate if connection to brokers is ready
        if not consumer.bootstrap_connected():
            LOGGER.error("Consumer failed to connect to brokers.")
            exit()

        return consumer

    def get_ml_avg_confidence_per_class(self, ml_confidence_all):
        tot_avg_per_class = {}
        
        for category, subdict in ml_confidence_all.items():
            conn_avg_all = []
            
            for _, values in subdict.items():
                if values:
                    conn_avg = sum(values) / len(values)
                    conn_avg_all.append(conn_avg)
            
            if conn_avg_all:
                class_avg = sum(conn_avg_all) / len(conn_avg_all)
            else:
                class_avg = 0
            
            tot_avg_per_class[category] = class_avg
            
        return tot_avg_per_class
    
    def add_records(self, records_queue, stop_event):
        while not stop_event.is_set():
            try:
                records = records_queue.get()
                #LOGGER.info(records)
                for record in records:
                    p = Point.from_dict(record, WritePrecision.MS)
                    self.write_api.write(self.influx_config.influx_bucket, self.influx_config.influx_org, p)
                self.write_api.flush()
                #LOGGER.info("Successfully added %s to InfluxDB database", records)
            except mp.queues.Empty:
                time.sleep(0.001)

    def start_monitoring(self):
        self.add_records_process.start()
        
        LOGGER.info("Starting DDOS detection...")
        time_interval_start = None
        time_interval_end = None
        records = []
        ml_confidence_all = {i: defaultdict(list) for i in self.label_correspondence.values()}
        tot_bytes_all = {i: 0 for i in self.label_correspondence.values()}
        tot_packets_all = {i: 0 for i in self.label_correspondence.values()}
        unique_hh_stats = {"hh_connection": set(), "hh_source": set(), "hh_target": set(), "attack_connection": set(), "ddos_attacker": set(), "ddos_target": set()}
        security_status = 0
        
        bytes_dict = defaultdict(int)
        packets_dict = defaultdict(int)
        
        n_snapshots = 0
        
        start_aggregating = None
        last_real_timestamp = 0
        last_nfstream_timestamp = 0
        counter = 0        
        
        while True:
            snapshots = list(self.consumer.poll().values())
            
            if snapshots:
                snapshots = snapshots[0]
                if start_aggregating is None:
                    start_aggregating = time.time()
                    
            else:
                time.sleep(0.0001)
                continue
            
            for snapshot in snapshots:
                
                data = snapshot.value
                
                labels = data['data']
                metadatas = data['metadata']
                
                version = snapshot.headers[0][1].decode("utf-8")
                
                for i in range(len(labels)):
                    n_snapshots += 1
                    counter +=1
                    label = labels[i]
                    metadata = metadatas[i]
                    
                    if counter % 10000 == 0:
                        LOGGER.info("Timestamps: nfstream (snapshot)=%s, nfstream (real)=%s, ai-inference=%s, ai-detector=%s, telemetry=%s", datetime.fromtimestamp(metadata["timestamp"]), datetime.fromtimestamp(metadata["timestamp_nfstream"]), datetime.fromtimestamp(metadata["timestamp_inference"]), datetime.fromtimestamp(metadata["timestamp_detector"]), datetime.now())
                    
                    if time_interval_start is None:
                        last_real_timestamp = datetime.fromtimestamp(metadata['timestamp_nfstream'])
                        #last_real_timestamp = datetime.fromtimestamp(metadata['timestamp'])
                        last_nfstream_timestamp = metadata['timestamp']
                        time_interval_start = datetime.fromtimestamp(metadata['timestamp_nfstream'])
                        time_interval_end = time_interval_start + timedelta(seconds=self.monitored_time_interval_agg)
                        
                    adjusted_time = last_real_timestamp + timedelta(seconds=metadata['timestamp']-last_nfstream_timestamp)
                    last_real_timestamp = datetime.fromtimestamp(metadata['timestamp_nfstream'])
                    last_nfstream_timestamp = metadata['timestamp']
                        
                    if adjusted_time >= time_interval_end:
                        LOGGER.info("Interval (%s, %s) ended", time_interval_start, time_interval_end)
                        LOGGER.info("Time to process every snapshot in the interval: %s", time.time() - start_aggregating)
                        LOGGER.info("%s snapshots processed", n_snapshots)
                        
                        n_snapshots = 0
                        time_to_plot = time_interval_end - timedelta(hours=2)
                        ml_avg_confidence_per_class = self.get_ml_avg_confidence_per_class(ml_confidence_all)
                        
                        for label_in_interval in self.label_correspondence.values():
                            
                            conn_data = {"measurement": "Connection data",
                                         "tags": {"Monitored device": self.monitored_device, "Interface": self.interface,
                                                  "Label": label_in_interval},
                                         "fields": {"Bytes in+out": tot_bytes_all[label_in_interval], "Packets in+out": tot_packets_all[label_in_interval]},
                                         "time": time_to_plot}
                            records.append(conn_data)
                            
                            if ml_avg_confidence_per_class[label_in_interval]!=0:
                                ml_avg_confidence = {"measurement": "ML Avg. Confidence",
                                                 "tags": {"Monitored device": self.monitored_device, "Interface": self.interface,
                                                          "Label": label_in_interval},
                                                 "fields": {"ML Avg. Confidence": ml_avg_confidence_per_class[label_in_interval]},
                                                 "time": time_to_plot}
                                records.append(ml_avg_confidence)

                        sec_status_record = {"measurement": "Security Status",
                                   "tags": {"Monitored device": self.monitored_device, "Interface": self.interface},
                                   "fields": {"Sec. Status": security_status},
                                   "time": time_to_plot}
                        attack_record = {"measurement": "Unique HH",
                                         "tags": {"Monitored device": self.monitored_device, "Interface": self.interface},
                                         "fields": {"Unique Attack Connections": len(unique_hh_stats["attack_connection"]),
                                                    "Unique DDoS Attackers": len(unique_hh_stats["ddos_attacker"]),
                                                    "Unique DDoS Targets": len(unique_hh_stats["ddos_target"])},
                                         "time": time_to_plot}
                        hh_record = {"measurement": "Unique HH",
                                     "tags": {"Monitored device": self.monitored_device, "Interface": self.interface},
                                     "fields": {"Unique HH Connections": len(unique_hh_stats["hh_connection"]), "Unique HH Sources": len(unique_hh_stats["hh_source"]),
                                                "Unique HH Targets": len(unique_hh_stats["hh_target"])},
                                     "time": time_to_plot}
                        records.append(attack_record)
                        records.append(hh_record)
                        records.append(sec_status_record)
                        
                        self.records_queue.put(records)
                        
                        time_interval_start = time_interval_end
                        time_interval_end = time_interval_start + timedelta(seconds=self.monitored_time_interval_agg)
                        
                        records = []

                        ml_confidence_all = {i: defaultdict(list) for i in self.label_correspondence.values()}
                        tot_bytes_all = {i: 0 for i in self.label_correspondence.values()}
                        tot_packets_all = {i: 0 for i in self.label_correspondence.values()}
                        unique_hh_stats = {"hh_connection": set(), "hh_source": set(), "hh_target": set(), "attack_connection": set(), "ddos_attacker": set(), "ddos_target": set()}
                        security_status = 0
                        start_aggregating = time.time()
                        
                    #start_calculating = time.time()
                    flow_bytes = int(metadata["flow_bytes"])
                    flow_packets = int(metadata["flow_pkts"])
                    #conn_id = (metadata['src_ip'], metadata['dst_ip'], metadata['src_port'], metadata['dst_port'], metadata['first'])
                    conn_id = (metadata['src_ip'], metadata['dst_ip'], metadata['src_port'], metadata['dst_port'])
                    
                    tot_delta_bytes_all = flow_bytes - bytes_dict[conn_id]

                    tot_delta_packets_all = flow_packets - packets_dict[conn_id]
                    
                    tot_bytes_all[label] += tot_delta_bytes_all
                    tot_packets_all[label] += tot_delta_packets_all
                    
                    bytes_dict[conn_id] = flow_bytes
                    packets_dict[conn_id] = flow_packets
                    
                    if label == self.label_correspondence["LABEL_HH_BENIGN"]:
                            unique_hh_stats["hh_connection"].add((metadata["src_ip"], metadata["dst_ip"], metadata["src_port"], metadata["dst_port"], metadata["first"]))
                            unique_hh_stats["hh_source"].add(metadata["src_ip"])
                            unique_hh_stats["hh_target"].add(metadata["dst_ip"])
                            
                            
                    if label == self.label_correspondence["LABEL_HH_MALIGN"]:
                            unique_hh_stats["attack_connection"].add((metadata["src_ip"], metadata["dst_ip"], metadata["src_port"], metadata["dst_port"], metadata["first"]))
                            unique_hh_stats["ddos_attacker"].add(metadata["src_ip"])
                            unique_hh_stats["ddos_target"].add(metadata["dst_ip"])
                            security_status = 1
                        
                    ml_confidence_all[label][conn_id].append(metadata['ml_confidence'])
                    
                    #LOGGER.info("Tiempo tardado en cálculo de métricas: %s", time.time() - start_calculating)
                

def main(args):
    """
    Main function for setting up and initialize data inference.
    Args:
        args (Any): Command-line arguments and options.
    """

    telemetry = AIMonitoring(kafka_url=args.kafka_url, consumer_topic=args.consumer_topic,
                                 consumer_client_id=args.consumer_client_id, consumer_group_id=args.consumer_group_id,
                                 influx_url=args.influx_url, influx_token=args.influx_token,
                                 influx_bucket=args.influx_bucket,
                                 influx_org=args.influx_org, influx_username=args.influx_username,
                                 influx_password=args.influx_password, time_interval=args.time_interval, max_poll_records=args.max_poll_records)
    telemetry.start_monitoring()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Initialize and start AI Monitoring component.")
    parser.add_argument("--kafka_url", type=str, default='localhost:9094',
                        help="IP address and port of the Kafka cluster ('localhost:9094' by default).")
    parser.add_argument("--consumer_topic", type=str, default='predicted_labels',
                        help="Kafka topic which the consumer will be consuming from.")
    parser.add_argument("--consumer_client_id", type=str, default='telemetry-consumer',
                        help="ID of the consumer client by which it will be recognized within Kafka.")
    parser.add_argument("--consumer_group_id", type=str, default='telemetry',
                        help="ID of the consumer group which the consumer belongs to.")
    parser.add_argument("--max_poll_records", type=int, default=500,
                        help="Max number of records to be polled.")
    parser.add_argument("--influx_url", type=str, default='http://localhost:8086',
                        help="URL pointing to the InfluxDB server.")
    parser.add_argument("--influx_token", type=str, default=None,
                        help="Auth token for InfluxDB server.")
    parser.add_argument("--influx_bucket", type=str, default='monitoring',
                        help="InfluxDB bucket where the data will be stored.")
    parser.add_argument("--influx_org", type=str, default='across',
                        help="InfluxDB organization.")
    parser.add_argument("--influx_username", type=str, default='admin',
                        help="InfluxDB username.")
    parser.add_argument("--influx_password", type=str, default='admin-lab',
                        help="InfluxDB password.")
    parser.add_argument("--time_interval", type=float, default=5.0,
                        help="Time interval.")

    args = parser.parse_args()
    main(args)
