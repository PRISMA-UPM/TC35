from typing import List
from time import sleep
from kafka import KafkaConsumer
from influxdb_client import WritePrecision, InfluxDBClient, Point, WriteOptions
from influxdb_client.client.write_api import SYNCHRONOUS
from datetime import datetime
import logging
import pickle
from collections import namedtuple
import time
import signal
import argparse
from sys import stdout, exit
import multiprocessing as mp
import requests
from message_processing import metrics_records_adder, calculate_metrics

# Setup LOGGER
LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.INFO)
logFormatter = logging.Formatter(fmt='%(asctime)s.%(msecs)03d %(levelname)-8s %(funcName)20s %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
consoleHandler = logging.StreamHandler(stdout)
consoleHandler.setFormatter(logFormatter)
LOGGER.addHandler(consoleHandler)

ConsumerConfig = namedtuple("ConsumerConfig", ["client_id", "group_id", "topic"])
InfluxConfig = namedtuple("InfluxConfig", ["influx_url", "influx_token", "influx_org", "influx_org_id", "influx_bucket", "influx_username", "influx_password"])

LABEL_CORRESPONDENCE = {"LABEL_HH_MALIGN": "HH Malign", "LABEL_HH_BENIGN": "HH Benign", "LABEL_STANDARD": "Standard", "LABEL_UNCLASSIFIED": "Unclassified"}


class AIMonitoring:
    def __init__(self, kafka_url: str, consumer_client_id: str, consumer_group_id: str, consumer_topic: str,
                 influx_url: str, influx_token: str, influx_org: str, influx_bucket: str, influx_username: str,
                 influx_password: str, time_interval: float):
        signal.signal(signal.SIGINT, self.handler)
        self.monitored_time_interval_agg = float(time_interval)
        self.broker = kafka_url
        self.consumer_config = ConsumerConfig(consumer_client_id, consumer_group_id, consumer_topic)
        LOGGER.info("Kafka consumer config: %s", self.consumer_config)
        self.consumer = self.connect_to_kafka()
        self.influx_client, self.write_api, influx_org_id = self.connect_to_influxdb(influx_url, influx_token, influx_org,
                                                                      influx_username, influx_password)
        self.influx_config = InfluxConfig(influx_url, influx_token, influx_org, influx_org_id, influx_bucket, influx_username, influx_password)
        LOGGER.info("InfluxDB config: %s", self.influx_config)
        self.stop_event = mp.Event()
        self.message_queue = mp.Queue()
        self.metrics_records_queue = mp.Queue()
        self.metrics_queue = mp.Queue()
        self.interval_queue = mp.Queue()
        self.message_processor_process = mp.Process(target=self.message_processor, args=(self.message_queue, self.metrics_queue, self.stop_event))
        self.metrics_records_adder_process = mp.Process(target=metrics_records_adder, args=(self.influx_config, self.write_api, self.metrics_records_queue, self.stop_event))
        self.calculate_metrics_process = mp.Process(target=calculate_metrics, args=(self.metrics_queue, self.metrics_records_queue, self.interval_queue, self.monitored_time_interval_agg, LABEL_CORRESPONDENCE, self.stop_event))

    def handler(self, num, frame):
        LOGGER.info("Gracefully stopping...")
        try:
            self.stop_event.set()
            self.calculate_metrics_process.join()
            self.calculate_stats_process.join()
            self.metrics_records_adder_process.join()
            self.stats_records_adder_process.join()
            self.message_processor_process.join()
            self.consumer.close()
            self.influx_client.close()
            exit()
        except:
            exit()

    def connect_to_influxdb(self, url: str, token: str, org_name: str, username: str, password: str):
        LOGGER.info("Attempting to connect to InfluxDB. url=%s, org=%s, token=%s, username=%s", url, org_name, token, username)
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
                                 group_id=self.consumer_config.group_id, value_deserializer=pickle.loads)

        LOGGER.info("Trying to establish connection to brokers...")
        LOGGER.info("Consumer connection status: %s", consumer.bootstrap_connected())

        # Validate if connection to brokers is ready
        if not consumer.bootstrap_connected():
            LOGGER.error("Consumer failed to connect to brokers.")
            exit()

        return consumer
         
    def message_processor(self, message_queue, metrics_queue, stop_event):
        while not stop_event.is_set():
            try:
                message = message_queue.get()
                metrics_queue.put(message)
            except mp.queues.Empty:
                pass
    

    def start_monitoring(self):
        LOGGER.info("Starting DDOS detection...")
        self.message_processor_process.start()
        self.metrics_records_adder_process.start()
        self.calculate_metrics_process.start()
        while True:
            try:
                messages = self.consumer.poll(10)
                if messages:
                    LOGGER.info("Received messages")
                    messages = list(messages.values())[0]
                    for message in messages:
                        data = message.value
                        self.message_queue.put(data)

            except Exception as e:
                LOGGER.error("Error during monitoring: %s: %s", type(e).__name__, e)
       


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
                                 influx_password=args.influx_password, time_interval=args.time_interval)
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
