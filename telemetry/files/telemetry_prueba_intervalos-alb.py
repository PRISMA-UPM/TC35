from typing import List
from time import sleep
from kafka import KafkaConsumer, KafkaAdminClient
from kafka.admin import NewTopic
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
import os
import csv
import numpy as np

# Setup LOGGER
LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.INFO)
logFormatter = logging.Formatter(fmt='%(asctime)s.%(msecs)03d %(levelname)-8s %(funcName)20s %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
consoleHandler = logging.StreamHandler(stdout)
consoleHandler.setFormatter(logFormatter)
LOGGER.addHandler(consoleHandler)

ConsumerConfig = namedtuple("ConsumerConfig", ["client_id", "group_id", "topic", "max_poll_records"])
InfluxConfig = namedtuple("InfluxConfig", ["influx_url", "influx_token", "influx_org", "influx_org_id", "influx_bucket", "influx_username", "influx_password"])
FIRST_TIMESTAMP_PCAP = 1720708093.927968
SECONDS_DELAY_GRAFANA = 0.0
LAST_INTERVAL = 144
NOMBRE_ARCHIVO_POR_SNAPSHOT = "output_csv/retrasos_snapshot.csv"
NOMBRE_ARCHIVO_POR_INTERVALO = "output_csv/retrasos_intervalo.csv"
NUM_PARTITIONS = 4

class AIMonitoring:
    def __init__(self, kafka_url: str, consumer_client_id: str, consumer_group_id: str, consumer_topic: str, max_poll_records: int,
                 influx_url: str, influx_token: str, influx_org: str, influx_bucket: str, influx_username: str,
                 influx_password: str, time_interval: float):
        signal.signal(signal.SIGINT, self.handler)
        admin = KafkaAdminClient(bootstrap_servers=args.kafka_url)
        
        #os.mkdir('output_csv')
        admin.create_topics([NewTopic("inference_data", num_partitions=NUM_PARTITIONS, replication_factor=1)])
        admin.create_topics([NewTopic("inference_probs", num_partitions=NUM_PARTITIONS, replication_factor=1)])
        admin.create_topics([NewTopic("predicted_labels", num_partitions=NUM_PARTITIONS, replication_factor=1)])
        admin.close()
        
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
                LOGGER.info("Successfully added %s to InfluxDB database", records)
            except mp.queues.Empty:
                time.sleep(0.001)
                
    #def write_in_csv_interval(self, file_name, l_counter_late, l_counter_rejected):
    def write_in_csv_interval(self, file_name, l_counter_late, l_counter_rejected, l_counter_snapshots):
        with open(file_name, mode='w', newline='') as archivo_csv:
            escritor_csv = csv.writer(archivo_csv)
            
            # Escribir los encabezados opcionalmente
            #escritor_csv.writerow(['l_counter_late_per_interval', 'l_counter_rejected_per_interval'])
            escritor_csv.writerow(['l_counter_late_per_interval', 'l_counter_rejected_per_interval', 'snapshots_per_interval'])
            
            # Escribir las filas
            #for val1, val2 in zip(l_counter_late, l_counter_rejected):
            #    escritor_csv.writerow([val1, val2])
            for val1, val2, val3 in zip(l_counter_late, l_counter_rejected,l_counter_snapshots):
                escritor_csv.writerow([val1, val2, val3])
                
    def write_in_csv_snapshots(self, file_name, lt_nfstream_inference, lt_inference_detector, lt_detector_monitoring, lt_total_delay):
        with open(file_name, mode='w', newline='') as archivo_csv:
            escritor_csv = csv.writer(archivo_csv)
            
            # Escribir los encabezados opcionalmente
            escritor_csv.writerow(['lt_nfstream_inference', 'lt_inference_detector', 'lt_detector_monitoring', 'lt_total_delay'])
            
            # Escribir las filas
            for val1, val2, val3, val4 in zip(lt_nfstream_inference, lt_inference_detector, lt_detector_monitoring, lt_total_delay):
                escritor_csv.writerow([val1, val2, val3, val4])

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
    
        
        start_aggregating = None
        last_real_timestamp = 0
        last_nfstream_timestamp = 0
        first_timestamp_real = 0
        first_timestamp_nfstream = 0
        counter = 0
        interval_counter = 0
        interval_start_relative = None
        interval_end_relative = None
        snapshots_per_class = defaultdict(int)
        
        first_timestamp_real = FIRST_TIMESTAMP_PCAP
        first_timestamp_nfstream = datetime.now() + timedelta(seconds=SECONDS_DELAY_GRAFANA)
        time_interval_start = first_timestamp_nfstream
        time_interval_end = time_interval_start + timedelta(seconds=self.monitored_time_interval_agg)
        interval_start_relative = 0
        interval_end_relative = interval_start_relative + self.monitored_time_interval_agg
        
        counter_late = 0
        counter_rejected = 0
        n_snapshots = 0
        
        #listas para medir retraso por snapshot
        lt_nfstream_inference = []
        lt_inference_detector = []
        lt_detector_monitoring = []
        lt_total_delay = []
        
        #listas para medir retraso por intervalos del monitoring. ACUMULADOS
        l_counter_snapshots = []   
        l_counter_late = []
        l_counter_rejected = []
             

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
                    timestamp_monitoring = datetime.timestamp(datetime.now())
                    
                    if counter % 10000 == 0:
                        #pass
                        #LOGGER.info("Timestamps: nfstream (snapshot)=%s, nfstream (real)=%s, ai-inference=%s, ai-detector=%s, telemetry=%s", datetime.fromtimestamp(metadata["timestamp"]), datetime.fromtimestamp(metadata["timestamp_nfstream"]), datetime.fromtimestamp(metadata["timestamp_inference"]), datetime.fromtimestamp(metadata["timestamp_detector"]), datetime.now())
                        LOGGER.info("Timestamps: nfstream=%s, ai-inference=%s, ai-detector=%s, telemetry=%s", datetime.fromtimestamp(metadata["timestamp_nfstream"]), datetime.fromtimestamp(metadata["timestamp_inference"]), datetime.fromtimestamp(metadata["timestamp_detector"]), datetime.fromtimestamp(timestamp_monitoring))
                        LOGGER.info("counter_late=%s, counter_rejected=%s", np.sum(np.array(l_counter_late)), np.sum(np.array(l_counter_rejected)))

                    adjusted_time = timedelta(seconds=metadata['timestamp'] - first_timestamp_real) + first_timestamp_nfstream
                    #LOGGER.info(f"Primer timestamp real: {datetime.fromtimestamp(first_timestamp_real)}, Primer timestamp nfstream: {first_timestamp_nfstream}, adjusted_time: {adjusted_time}, timestamp actual real: {datetime.fromtimestamp(metadata['timestamp'])}")                
                    
                    if adjusted_time < time_interval_start:
                        counter_late += 1
                        #LOGGER.info(f"SNAPSHOT LLEGA TARDE:{adjusted_time} ({time_interval_start}, {time_interval_end}). {counter_late}. {counter_rejected}")
                        if adjusted_time < (time_interval_start - timedelta(seconds=5)):
                            counter_rejected +=1
                            continue
                        #LOGGER.info(f"SNAPSHOT LLEGA TARDE:{adjusted_time}. TOTAL: {counter_late}")
                        
                    if adjusted_time >= time_interval_end:
                        LOGGER.info("Interval (%s, %s) ended", time_interval_start, time_interval_end)
                        LOGGER.info("Time to process every snapshot in the interval: %s", time.time() - start_aggregating)
                        LOGGER.info("%s snapshots processed", n_snapshots)
                        LOGGER.info(f"snapshots per class: {snapshots_per_class}")
                        LOGGER.info(f"INTERVAL: {interval_counter}, ({interval_start_relative}, {interval_end_relative})")
                        
                        l_counter_snapshots.append(n_snapshots)
                        l_counter_late.append(counter_late)
                        l_counter_rejected.append(counter_rejected)
                        
                        n_snapshots = 0
                        counter_late = 0
                        counter_rejected= 0
                        interval_counter += 1
                        
                        if interval_counter == LAST_INTERVAL:
                            
                            #self.write_in_csv_interval(NOMBRE_ARCHIVO_POR_INTERVALO, l_counter_late, l_counter_rejected)
                            self.write_in_csv_interval(NOMBRE_ARCHIVO_POR_INTERVALO, l_counter_late, l_counter_rejected, l_counter_snapshots)
                            self.write_in_csv_snapshots(NOMBRE_ARCHIVO_POR_SNAPSHOT, lt_nfstream_inference, lt_inference_detector, lt_detector_monitoring, lt_total_delay)                        
                        
                        time_to_plot = time_interval_end - timedelta(hours=2)
                        ml_avg_confidence_per_class = self.get_ml_avg_confidence_per_class(ml_confidence_all)
                        
                        #ESCRITURA EN EL INFLUX POR CLASES
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
                        
                        #ESCRITURA EN EL INFLUX DATOS GLOBALES
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
                        
                        #RESETEAR CONTADORES PARA NUEVO INTERVALO
                        
                        time_interval_start = time_interval_end
                        time_interval_end = time_interval_start + timedelta(seconds=self.monitored_time_interval_agg)
                        interval_start_relative = interval_end_relative
                        interval_end_relative = interval_start_relative + self.monitored_time_interval_agg
                        
                        records = []

                        ml_confidence_all = {i: defaultdict(list) for i in self.label_correspondence.values()}
                        tot_bytes_all = {i: 0 for i in self.label_correspondence.values()}
                        tot_packets_all = {i: 0 for i in self.label_correspondence.values()}
                        unique_hh_stats = {"hh_connection": set(), "hh_source": set(), "hh_target": set(), "attack_connection": set(), "ddos_attacker": set(), "ddos_target": set()}
                        security_status = 0
                        start_aggregating = time.time()
                    
                    #CALCULAR ESTADÍSTICAS PARA CADA SNAPSHOT
                      
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
                    snapshots_per_class[label] +=1
                    
                    #AÑADIR DIFERENCIAS DE TIEMPO ENTRE COMPONENTES POR SNAPSHOT
                    
                    lt_nfstream_inference.append(metadata['timestamp_inference'] - metadata['timestamp_nfstream'])
                    lt_inference_detector.append(metadata['timestamp_detector'] - metadata['timestamp_inference'])
                    lt_detector_monitoring.append(timestamp_monitoring - metadata['timestamp_detector'])
                    lt_total_delay.append(timestamp_monitoring - datetime.timestamp(time_interval_end))
                    
                    #ACTUALIZAR ACUMULADORES DE HH_BENIGN Y hh_MALIGN POR SNAPSHOT
                    
                    if label == self.label_correspondence["LABEL_HH_BENIGN"]:
                            unique_hh_stats["hh_connection"].add((metadata["src_ip"], metadata["dst_ip"], metadata["src_port"], metadata["dst_port"]))
                            unique_hh_stats["hh_source"].add(metadata["src_ip"])
                            unique_hh_stats["hh_target"].add(metadata["dst_ip"])
                            
                            
                    if label == self.label_correspondence["LABEL_HH_MALIGN"]:
                            unique_hh_stats["attack_connection"].add((metadata["src_ip"], metadata["dst_ip"], metadata["src_port"], metadata["dst_port"]))
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
