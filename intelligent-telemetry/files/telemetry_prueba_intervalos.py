from typing import List
from time import sleep
from kafka import KafkaConsumer, KafkaAdminClient
from kafka.admin import NewTopic
from influxdb_client import WritePrecision, InfluxDBClient, Point, WriteOptions
from influxdb_client.client.write_api import SYNCHRONOUS
from datetime import datetime, timedelta
import logging
#import pickle
import dill as pickle
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

import cProfile
import pstats

from collections import namedtuple

HORARIO_VERANO=False

# Setup LOGGER
LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.INFO)
logFormatter = logging.Formatter(fmt='%(asctime)s.%(msecs)03d %(levelname)-8s %(funcName)20s %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
consoleHandler = logging.StreamHandler(stdout)
consoleHandler.setFormatter(logFormatter)
LOGGER.addHandler(consoleHandler)

ConsumerConfig = namedtuple("ConsumerConfig", ["client_id", "group_id", "topic", "max_poll_records"])
InfluxConfig = namedtuple("InfluxConfig", ["influx_url", "influx_token", "influx_org", "influx_org_id", "influx_bucket", "influx_username", "influx_password"])
FIRST_TIMESTAMP_PCAP = -1 # no se utiliza 1720708093.927968
SECONDS_DELAY_GRAFANA = 0.0
LAST_INTERVAL = 1000 #150 #70 #150
INT_DEBUG=9999 #60
NOMBRE_ARCHIVO_POR_SNAPSHOT = "output_csv/retrasos_snapshot.csv"
NOMBRE_ARCHIVO_POR_INTERVALO = "output_csv/retrasos_intervalo.csv"
NUM_PARTITIONS = 8 #4

T_SLEEP_NO_MENSAJES= 0.001 # antes estaba en 0.5

MAX_rafaga_snapshots_futuro = 7500 # 750 #250 #1000 # 500 #10000 #500 # 2000 #500 #250 # 500 # 100

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

        # Para crear fichero csv
        self.crear_csv = True

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

    def escribir_influx (self,time_to_plot, tot_bytes_all,tot_packets_all,ml_avg_confidence_per_class,security_status,unique_hh_stats):
                        
        records=[]    
        
        #ESCRITURA EN EL INFLUX: DATOS DEL INTERVALO POR CLASES 
        for label_in_interval in self.label_correspondence.values():
            #DEBUG
            LOGGER.info (f"to INFLUSHDB label: {label_in_interval} Bytes in+out : {tot_bytes_all[label_in_interval]}, Packets in+out: {tot_packets_all[label_in_interval]}")
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
            
        #ESCRITURA EN EL INFLUX: DATOS GLOBALES
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
        records.append(sec_status_record)
        records.append(attack_record)
        records.append(hh_record)

        
        self.records_queue.put(records)
                        

    def start_monitoring(self):
        self.add_records_process.start()
        
        LOGGER.info("Start monitoring ...")
        time_interval_start = None
        time_interval_end = None
        
        ml_confidence_all = {i: defaultdict(list) for i in self.label_correspondence.values()}
        tot_bytes_all = {i: 0 for i in self.label_correspondence.values()}
        tot_packets_all = {i: 0 for i in self.label_correspondence.values()}
        tot_snap_all = {i: 0 for i in self.label_correspondence.values()}
        unique_hh_stats = {"hh_connection": set(), "hh_source": set(), "hh_target": set(), "attack_connection": set(), "ddos_attacker": set(), "ddos_target": set()}
        security_status = 0.0
        num_malign_interval = 0
        
        bytes_dict = defaultdict(int)
        packets_dict = defaultdict(int)
        snapshots_per_class_int = defaultdict(int)
        snapshots_per_class = defaultdict(int)
        
        start_aggregating = None
        last_real_timestamp = 0
        last_nfstream_timestamp = 0
        first_timestamp_real = 0
        first_timestamp_nfstream = 0      
        interval_counter = 0
        interval_start_relative = None
        interval_end_relative = None
        
        
        first_timestamp_real = FIRST_TIMESTAMP_PCAP
        first_timestamp_nfstream = datetime.now() + timedelta(seconds=SECONDS_DELAY_GRAFANA)
        time_interval_start = first_timestamp_nfstream
        time_interval_end = time_interval_start + timedelta(seconds=self.monitored_time_interval_agg)
        interval_start_relative = 0
        interval_end_relative = interval_start_relative + self.monitored_time_interval_agg

        n_snapshots_tot = 0 # antigua variable counter
        n_snapshots_int = 0
        n_snapshots_raw_int = 0 # Si se procesa algo de pendientes varias vces, no se descuenta
        n_snapshots_late_tot = 0
        n_snapshots_late_int = 0
        n_snapshots_rejected_int = 0
        n_snapshots_rejected_tot = 0
        n_snapshots_dentro_int= 0
        n_snapshots_dentro_tot= 0

        n_snapshots_leidos_kafka= 0
        
        #listas para medir retraso por snapshot
        lt_nfstream_inference = []
        lt_inference_detector = []
        lt_detector_monitoring = []
        lt_total_delay = []
        
        #listas para medir retraso por intervalos del monitoring. ACUMULADOS
        l_counter_snapshots = []   
        l_counter_late = []
        l_counter_rejected = []

        labels_pendientes= []
        metadatas_pendientes= []
        
        # Para contar la rafaga de snapshots que llegan antes (por delante del interalo actual). 
        # Si la rafaga es grande se puede suponer que ya no van a llegar snapshots de este intervalo y hay que crear un intervalo nuevo
        #MAX_rafaga_snapshots_futuro = 10000
        num_rafaga_snapshots_futuro=0
        
        # Tiempo Ultimo mensaje en intervalo. Para detectar saltos de intervalo cuando no llegan mas snapshot en intervalo actual
        t_ultimo_snap_intervalo=datetime.now()
        
        start_aggregating = time.time() 
        l_t_proc_int=[]
        
        LOGGER.info (f"Arranco while. Date:{datetime.now()}")

        #min_diff=999999999
        primer_mensaje= True
        
        # Se procesan solo una vez por intervalo
        pendientes_procesadas=False

        # -------------
        # PROFILING 
        t_poll=0.0
        n_polls=0
        
        t_not_mensajes= 0.0
        n_not_mensajes=0

        t_not_pendientes_procesadas=0.0
        n_not_pendientes_procesadas=0

        t_snap_fuera_int=0.0
        n_snap_fuera_int=0

        t_snap_dentro_int=0.0
        n_snap_dentro_int=0

        l_record_profiling =[]
        # Define the record
        MetricsRecord = namedtuple('MetricsRecord', ['interval_counter', 
                                                     't_poll', 'n_polls', 't_not_mensajes', 'n_not_mensajes', 't_not_pendientes_procesadas', 'n_not_pendientes_procesadas', 
                                                     't_snap_fuera_int', 'n_snap_fuera_int', 't_snap_dentro_int', 'n_snap_dentro_int'])


        # -------------

        if self.crear_csv:
            file_snap = open('output_csv/snapshots.csv', 'w')
            header = "timestamp*adjusted_timestamp*timestamp_nfstream*timestamp_ai_mon_get_kafka*timestamp_inference*timestamp_ai_mon_put_kafka*timestamp_det_get_kafka*timestamp_detector*timestamp_det_put_kafka*timestamp_ai_mon_get_kafka*timestamp_ai_mon_proc_int*src_ip*dst_ip*src_port*dst_port*protocol"
            file_snap.write (header+"\n")


        if (interval_counter ==INT_DEBUG): LOGGER.info ("entro en 'while True:'")
        while True:
            t1_while_main=time.time()
            
            if (interval_counter ==INT_DEBUG): LOGGER.info (f"before self.consumer.poll(). Date:{datetime.now()}")
            t1=time.time()
            mensajes = list(self.consumer.poll().values())
            t_get_kafka=time.time()
            t2=time.time()
            t_poll+= t2-t1
            n_polls+=1
            

            if (interval_counter ==INT_DEBUG): LOGGER.info (f"after self.consumer.poll(). Date:{datetime.now()}")

            #if not mensajes:
            if not mensajes and ( (len(labels_pendientes) == 0) or pendientes_procesadas) :
                n_not_mensajes+=1
                t1=time.time()
                t1_if_then=time.time()
                # --------------------
                # NO hay mensajes
                # --------------------
                if (interval_counter ==INT_DEBUG): LOGGER.info ("No hay mensajes.")
                now= datetime.now()
                
                # time_interval_end + timedelta(seconds=5*self.monitored_time_interval_agg))  
                #LOGGER.info (f"True o False: now {now} > time_interval_end {time_interval_end} and ((now {now} - t_ultimo_snap_intervalo {t_ultimo_snap_intervalo}) > 1 sec ")
                
                if  not primer_mensaje and (now > time_interval_end ) and ((now - t_ultimo_snap_intervalo) > timedelta(seconds=1)):   
                    #
                    # Crear nuevo intervalo   
                    #
                    
                    #LOGGER.info (f"Now:{now}, time_interval_end:{time_interval_end}, time_interval_start:{time_interval_start}")
                    LOGGER.info (f"Create interval after consumer.poll. There are not messages now {now} > time_interval_end {time_interval_end} and (n_snapshots_int {n_snapshots_int} < 10)")
                    
                    num_pendientes= len(labels_pendientes)
                    t_proc_int= round(time.time() - start_aggregating,2)
                   
                    l_t_proc_int.append(f"N:{t_proc_int}:{n_snapshots_raw_int}")

                    t_med_intervalos = round(np.mean(np.array([float(item.split(':')[1]) for item in l_t_proc_int])),2)
                    
                    # Log de intervalo anterior
                    LOGGER.info ("-------------------------------------------------------")
                    LOGGER.info (f"INTERVAL: {interval_counter}, ({interval_start_relative}, {interval_end_relative}) finalised")
                    LOGGER.info ("Absolute times: Interval (%s, %s)", time_interval_start, time_interval_end)
                    LOGGER.info (f"Now: {datetime.now()}")
                    LOGGER.info (f"Time to process the interval: {t_proc_int}. Mean time:{t_med_intervalos}" )
                    LOGGER.info (f"Within current interval: Snapshots processed: all(n_snapshots_int): {n_snapshots_int} = inside:{n_snapshots_dentro_int} (late: {n_snapshots_late_int}) +  rejected:{n_snapshots_rejected_int} + Num pending len(labels_pendientes):{num_pendientes}")
                    LOGGER.info (f"Within current interval: snapshot streak outside interval:{num_rafaga_snapshots_futuro} Num pending len(labels_pendientes):{num_pendientes}.")
                    LOGGER.info (f"TOTAL snapshots per class: {snapshots_per_class}")
                    LOGGER.info (f"TOTAL snapshots: all(n_snapshots_tot): {n_snapshots_tot} = inside:{n_snapshots_dentro_tot} (late: {n_snapshots_late_tot}) + rejected:{n_snapshots_rejected_tot} + Num pending len(labels_pendientes):{num_pendientes}")
                    LOGGER.info (f"TOTAL snapshots/labels read from kafka:{n_snapshots_leidos_kafka}")
                    #LOGGER.info (f"Tiempos proceso en intervalos:{l_t_proc_int}")
                    LOGGER.info ("-------------------------------------------------------\n")
                    
                    # Log de estadisticas de snapshots descolocados
                    l_counter_snapshots.append(n_snapshots_int)
                    l_counter_late.append(n_snapshots_late_int)
                    l_counter_rejected.append(n_snapshots_rejected_int)

                    num_rafaga_snapshots_futuro=0
                    n_snapshots_int = 0
                    n_snapshots_raw_int = 0
                    n_snapshots_late_int = 0
                    n_snapshots_rejected_int= 0
                    n_snapshots_dentro_int= 0
                    interval_counter += 1
                    #counter-= num_pendientes

                    # Se procesan solo una vez por intervalo
                    pendientes_procesadas=False
                    
                    
                    # 

                    if HORARIO_VERANO:
                        # Horario de verano
                        time_to_plot = time_interval_end - timedelta(hours=2)
                    else:
                        # Horario de invierno
                        time_to_plot = time_interval_end - timedelta(hours=1)
                    
                    ml_avg_confidence_per_class = self.get_ml_avg_confidence_per_class(ml_confidence_all)
                    
                    #ESCRITURA EN EL INFLUX: DATOS DEL INTERVALO POR CLASES y DATOS GLOBALES
                    self.escribir_influx (time_to_plot, tot_bytes_all,tot_packets_all,ml_avg_confidence_per_class,security_status,unique_hh_stats)
                    #self.escribir_influx (time_to_plot, snapshots_per_class_int,tot_packets_all,ml_avg_confidence_per_class,security_status,unique_hh_stats)
                            
                    
                    #RESETEAR CONTADORES PARA NUEVO INTERVALO
                    
                    time_interval_start = time_interval_end
                    time_interval_end = time_interval_start + timedelta(seconds=self.monitored_time_interval_agg)
                    #tiempos relativos intervalo a partir de t=0
                    interval_start_relative = interval_end_relative
                    interval_end_relative = interval_start_relative + self.monitored_time_interval_agg

                    # Reseteo de contadors globales
                    ml_confidence_all = {i: defaultdict(list) for i in self.label_correspondence.values()}
                    tot_bytes_all = {i: 0 for i in self.label_correspondence.values()}
                    tot_packets_all = {i: 0 for i in self.label_correspondence.values()}
                    snapshots_per_class_int = {i: 0 for i in self.label_correspondence.values()}
                    unique_hh_stats = {"hh_connection": set(), "hh_source": set(), "hh_target": set(), "attack_connection": set(), "ddos_attacker": set(), "ddos_target": set()}
                    security_status = 0.0
                    num_malign_interval = 0
                    start_aggregating = time.time()
                    if (interval_counter ==INT_DEBUG): LOGGER.info ("Start aggregating interval")

                    # Acumula debugging por intervalo y resetea contadores
                    record_profiling = MetricsRecord (interval_counter=interval_counter,
                                        t_poll=t_poll, n_polls=n_polls,
                                        t_not_mensajes=t_not_mensajes, n_not_mensajes=n_not_mensajes,
                                        t_not_pendientes_procesadas=t_not_pendientes_procesadas, n_not_pendientes_procesadas=n_not_pendientes_procesadas,
                                        t_snap_fuera_int=t_snap_fuera_int, n_snap_fuera_int=n_snap_fuera_int,
                                        t_snap_dentro_int=t_snap_dentro_int, n_snap_dentro_int=n_snap_dentro_int
                                    )
                    l_record_profiling.append (record_profiling)
                    #
                    t_poll=0.0; n_polls=0
                    t_not_mensajes= 0.0; n_not_mensajes=0
                    t_not_pendientes_procesadas=0.0; n_not_pendientes_procesadas=0
                    t_snap_fuera_int=0.0; n_snap_fuera_int=0
                    t_snap_dentro_int=0.0; n_snap_dentro_int=0

                    if interval_counter == LAST_INTERVAL:
                        self.write_in_csv_interval(NOMBRE_ARCHIVO_POR_INTERVALO, l_counter_late, l_counter_rejected, l_counter_snapshots)
                        self.write_in_csv_snapshots(NOMBRE_ARCHIVO_POR_SNAPSHOT, lt_nfstream_inference, lt_inference_detector, lt_detector_monitoring, lt_total_delay)      
                        #LOGGER.info (f"l_record_profiling : {l_record_profiling}")
                        with open('metrics_records.pkl', 'wb') as f:
                            pickle.dump(l_record_profiling, f)            
                        return
                else:    
                    time.sleep(T_SLEEP_NO_MENSAJES)
                    t2=time.time()
                    t_not_mensajes+=t2-t1
                    if (interval_counter ==INT_DEBUG): LOGGER.info (f"Dormir:{T_SLEEP_NO_MENSAJES} ....")
                        
                t2_if_then=time.time()
                if (interval_counter ==INT_DEBUG): LOGGER.info (f"t 'if not mensajes':{t2_if_then-t1_if_then}")

            else:
                # --------------------
                # Hay mensajes
                # --------------------
                t1_if_else=time.time()
                
                if (interval_counter ==INT_DEBUG): LOGGER.info (f"Hay mensajes. Num Mensajes:{len(mensajes)}")
                #LOGGER.info (f"Mensaje:{mensajes}")
                #LOGGER.info ("-------------------")
                #LOGGER.info (f"mensaje[0]:{mensajes[0]}")
                #LOGGER.info ("-------------------")
                
                if primer_mensaje:
                    primer_mensaje= False
                    # Cada mensaje es un array de consumer_record
                    a_consumer_records = mensajes[0]
                    consumer_record= a_consumer_records[0]
                    metadatas=consumer_record.value["metadata"]
                    first_timestamp_real= metadatas[0]['timestamp']
                    first_timestamp_nfstream = datetime.now() + timedelta(seconds=SECONDS_DELAY_GRAFANA)
                    time_interval_start = first_timestamp_nfstream
                    time_interval_end = time_interval_start + timedelta(seconds=self.monitored_time_interval_agg)
                    start_aggregating = time.time()
                    LOGGER.info (f"Primer intervalo. time_interval_start:{time_interval_start} time_interval_end:{time_interval_end}")
                    LOGGER.info (f"first_timestamp_real: {first_timestamp_real}, FIRST_TIMESTAMP_PCAP:{FIRST_TIMESTAMP_PCAP} ")
                
                if (interval_counter ==INT_DEBUG): LOGGER.info (f"ini 'for mensaje in mensajes' num mensajes:{len(mensajes)}")   
                t1_for=time.time()
                for mensaje in mensajes:
                    # mensaje viene como un array de 1 elemento [ConsumerRecord]
                    a_consumer_records=mensaje
                    #if (interval_counter ==INT_DEBUG): LOGGER.info (f"Hay consumer records. Num CR:{len(a_consumer_records)}")
                    if (interval_counter ==INT_DEBUG): LOGGER.info (f"ini 'for consumer_record in a_consumer_records'. num CRs:{len(a_consumer_records)}") 
                    for consumer_record in a_consumer_records: 
                        #if (interval_counter ==INT_DEBUG): LOGGER.info (f"Procesa CR. {consumer_record}")
                            
                        # lista de labels y metadatas de un batch de prediccion (son las prediciones y metadatos de una lista de snapshots)
                        labels = consumer_record.value['data']
                        n_snapshots_leidos_kafka+= len (labels)
                        if (interval_counter ==INT_DEBUG): LOGGER.info (f"Num snapshots/labels en CR:{len(labels)}")
                        #if (interval_counter ==INT_DEBUG): LOGGER.info (f"labels: {labels}, tipo:{type(labels)}")
                        metadatas = consumer_record.value['metadata']
            
                        #LOGGER.info (f"OOOO data:{data}")
                        #LOGGER.info (f"OOOO data['data'] labels:{labels}")
                        #LOGGER.info (f"OOOO data['metadata'] metadatas:{metadatas}")
                        
                        # Por si quedan pendientes de una ronda anterior
                        if not pendientes_procesadas:
                            t1=time.time()
                            pendientes_procesadas=True
                            npend=len(labels_pendientes)
                            if len(labels_pendientes) > 0 : 
                                #LOGGER.info (f"labels_pendientes ronda anterior: {len(labels_pendientes)}")
                                n_snapshots_int-=len(labels_pendientes)
                                n_snapshots_tot-=len(labels_pendientes)
                                #LOGGER.info (f"(antes extend) pendientes:{labels_pendientes}: labels:{labels}")
                                labels_pendientes.extend(labels)
                                labels= labels_pendientes
                                metadatas_pendientes.extend(metadatas)
                                metadatas = metadatas_pendientes
                                #LOGGER.info (f"(despues extend) pendientes:{labels_pendientes}: labels:{labels}")
                                labels_pendientes=[]
                                metadatas_pendientes=[]
                            t2=time.time()
                            t_not_pendientes_procesadas+=t2-t1
                            n_not_pendientes_procesadas+=1

                            if (interval_counter ==INT_DEBUG): LOGGER.info (f"Procesar pendientes: {npend}. time:{t2-t1}")
                    
                                
                        # ---------------
                        # Procesa labels 
                        # ---------------
                        #
                        i = 0
                        if (interval_counter ==INT_DEBUG): LOGGER.info (f"ini while i {i} < len(labels):{len(labels)}")
                        n_snap_next_w = 0
                        n_snap_dentro_w = 0
                        t1_while=time.time()
                        while i < len(labels):
                            
                            t1=time.time()
                            #LOGGER.info (f"XXXX racha de snapshots fuera. num_rafaga_snapshots_futuro:{num_rafaga_snapshots_futuro}  ")
                            n_snapshots_int += 1
                            n_snapshots_raw_int += 1
                            n_snapshots_tot += 1
                            label = labels[i]
                            metadata = metadatas[i]
                            #if (interval_counter ==INT_DEBUG): LOGGER.info (f"XXXX dentro de while i:{i} < len(labels):{len(labels)}. label:{label}")
                            #if (interval_counter ==INT_DEBUG): LOGGER.info (f"metadata:{metadata}")
                                
                            timestamp_monitoring = datetime.timestamp(datetime.now())
                
                            adjusted_time = timedelta(seconds=metadata['timestamp'] - first_timestamp_real) + first_timestamp_nfstream
                            t_pcap=metadata['timestamp']
                            #t2=first_timestamp_real
                            diff=metadata['timestamp'] - first_timestamp_real
                           
                            '''
                            Para debugging snapshot
                            #LOGGER.info (f"XXXX timestamp pkt: {t_pcap}. first_timestamp_real {first_timestamp_real}. diff: {diff}. diff:{timedelta(seconds=metadata['timestamp'] - first_timestamp_real)}. Min diff:{min_diff}, snapshot num:{min_snap}" )
                            #LOGGER.info ("XXXX Timestamps: nfstream=%s, ai-inference=%s, ai-detector=%s, telemetry=%s", datetime.fromtimestamp(metadata["timestamp_nfstream"]), datetime.fromtimestamp(metadata["timestamp_inference"]), datetime.fromtimestamp(metadata["timestamp_detector"]), datetime.fromtimestamp(timestamp_monitoring))
                            #LOGGER.info (f"XXXX adjusted_time snapshot:{adjusted_time}. < time_interval_end:{time_interval_end} ?")
                            '''
                            #LOGGER.info (f"XXXX metadata['timestamp']: {metadata['timestamp']}. first_timestamp_real {first_timestamp_real}. diff: {diff}. diff:{timedelta(seconds=metadata['timestamp'] - first_timestamp_real)}. " )
                            #LOGGER.info (f"XXXX adjusted_time snapshot:{adjusted_time}. < time_interval_end:{time_interval_end} ?")
                            
                            '''
                            if n_snapshots_tot % 100000 == 0:
                                #pass
                                #LOGGER.info("Timestamps: nfstream (snapshot)=%s, nfstream (real)=%s, ai-inference=%s, ai-detector=%s, telemetry=%s", datetime.fromtimestamp(metadata["timestamp"]), datetime.fromtimestamp(metadata["timestamp_nfstream"]), datetime.fromtimestamp(metadata["timestamp_inference"]), datetime.fromtimestamp(metadata["timestamp_detector"]), datetime.now())
                                LOGGER.info ("-----")
                                LOGGER.info (f"Snapshot number:{n_snapshots_tot}")
                                LOGGER.info ("Timestamps: nfstream=%s, ai-inference=%s, ai-detector=%s, telemetry=%s", datetime.fromtimestamp(metadata["timestamp_nfstream"]), datetime.fromtimestamp(metadata["timestamp_inference"]), datetime.fromtimestamp(metadata["timestamp_detector"]), datetime.fromtimestamp(timestamp_monitoring))
                                LOGGER.info (f"adjusted_time snapshot:{adjusted_time}. < time_interval_end:{time_interval_end} ?")
                                LOGGER.info (f"En intervalo actual: Snapshots processed: todos: {n_snapshots_int}, dentro:{n_snapshots_dentro_int}, late: {n_snapshots_late_int}, rejected:{n_snapshots_rejected_int}")
                                LOGGER.info (f"En intervalo actual: racha de snapshots fuera:{num_rafaga_snapshots_futuro} Num pendientes len(labels_pendientes):{len(labels_pendientes)}.")
                                LOGGER.info ("Snapshot Stats. TOTAL: snapshots=%s, late=%s, rejected=%s", n_snapshots_tot, np.sum(np.array(l_counter_late)), np.sum(np.array(l_counter_rejected)))
                                LOGGER.info (f"TOTAL snapshots per class: {snapshots_per_class}")
                                LOGGER.info (f"TOTAL snapshots: todos: {n_snapshots_tot}, dentro:{n_snapshots_dentro_tot}, late: {n_snapshots_late_tot}, rejected:{n_snapshots_rejected_tot}")
                                LOGGER.info (f"TOTAL snapshots/labels leidos de kafka:{n_snapshots_leidos_kafka}")
                                LOGGER.info ("-----")
                            '''
                            #LOGGER.info(f"Primer timestamp real: {datetime.fromtimestamp(first_timestamp_real)}, Primer timestamp nfstream: {first_timestamp_nfstream}, adjusted_time: {adjusted_time}, timestamp actual real: {datetime.fromtimestamp(metadata['timestamp'])}")                
                                                                    
                            if adjusted_time >= time_interval_end:

                                if (interval_counter ==INT_DEBUG): LOGGER.info (f"ZZZZ Snapshot llega demasiado pronto. tam rafaga:{num_rafaga_snapshots_futuro}")
                                n_snap_next_w+=1
                                    
                                t11=time.time()
                                # Llega demasiado pronto
                                #LOGGER.info ("ZZZZ Timestamps: nfstream=%s, ai-inference=%s, ai-detector=%s, telemetry=%s", datetime.fromtimestamp(metadata["timestamp_nfstream"]), datetime.fromtimestamp(metadata["timestamp_inference"]), datetime.fromtimestamp(metadata["timestamp_detector"]), datetime.fromtimestamp(timestamp_monitoring))
                                #LOGGER.info (f"ZZZZ adjusted_time snapshot:{adjusted_time}. >= time_interval_end:{time_interval_end} ?")
                                labels_pendientes.append(label)
                                metadatas_pendientes.append (metadata)
                                num_rafaga_snapshots_futuro+= 1                   
                
                                # Miramos a ver si hay que crear un intervalo nuevo
                                now=datetime.now() 
                                #if ( (now > time_interval_end) and (n_snapshots < 10)) or (num_rafaga_snapshots_futuro > MAX_rafaga_snapshots_futuro) :
                                #if (num_rafaga_snapshots_futuro > MAX_rafaga_snapshots_futuro) :
                                if (num_rafaga_snapshots_futuro > MAX_rafaga_snapshots_futuro) and ( (time.time() - start_aggregating) > (0.85*self.monitored_time_interval_agg) ):
                                    #
                                    # Crear nuevo intervalo   
                                    #
                                    if now > time_interval_end:
                                        LOGGER.info (f"Create new interval. Interval end reached. Now: {now} > time_interval_end {time_interval_end} and (n_snapshots {n_snapshots_int} < 10)")
                                    if num_rafaga_snapshots_futuro > MAX_rafaga_snapshots_futuro:
                                        LOGGER.info (f"Create new interval. Only intervals from the next snapshot are collected num_rafaga_snapshots_futuro:{num_rafaga_snapshots_futuro}")
                                        
                                    num_pendientes= len(labels_pendientes)
                                    # Meter las pendientes (labels y metadatas) para su reprocesamiento en el intervalo nuevo
                                    labels_pendientes.extend(labels[i+1:])
                                    labels= labels_pendientes
                                    metadatas_pendientes.extend(metadatas[i+1:])
                                    metadatas=metadatas_pendientes
                                    labels_pendientes=[] # .clear()
                                    metadatas_pendientes= [] #.clear()
                
                                    # Se procesan solo una vez por intervalo. Aqui no haria falta, porque ya hemos metido las pendientes en labels
                                    # pendientes_procesadas=False
                            
                                    i = 0  # Reset index to start processing from the beginning

                                    t_proc_int= round(time.time() - start_aggregating,2)
                                    l_t_proc_int.append(f"S:{t_proc_int}:{n_snapshots_raw_int}")
                                    t_med_intervalos = round(np.mean(np.array([float(item.split(':')[1]) for item in l_t_proc_int])),2)
                                    
                                    # Log de intervalo anterior
                                    LOGGER.info ("-------------------------------------------------------")
                                    LOGGER.info (f"INTERVAL: {interval_counter}, ({interval_start_relative}, {interval_end_relative}) finalised")
                                    LOGGER.info ("Absolute times: Interval (%s, %s)", time_interval_start, time_interval_end)
                                    LOGGER.info (f"Now: {datetime.now()}")
                                    LOGGER.info (f"Time to process the interval: {t_proc_int}. Mean time:{t_med_intervalos}" )
                                    LOGGER.info (f"Within current interval: Snapshots processed: all(n_snapshots_int): {n_snapshots_int} = inside:{n_snapshots_dentro_int} (late: {n_snapshots_late_int}) +  rejected:{n_snapshots_rejected_int} + Num pending len(labels_pendientes):{num_pendientes}")
                                    LOGGER.info (f"Within current interval: snapshot streak outside interval:{num_rafaga_snapshots_futuro} Num pending len(labels_pendientes):{num_pendientes}.")
                                    LOGGER.info (f"TOTAL snapshots per class: {snapshots_per_class}")
                                    LOGGER.info (f"TOTAL snapshots: all(n_snapshots_tot): {n_snapshots_tot} = inside:{n_snapshots_dentro_tot} (late: {n_snapshots_late_tot}) + rejected:{n_snapshots_rejected_tot} + Num pending len(labels_pendientes):{num_pendientes}")
                                    LOGGER.info (f"TOTAL snapshots/labels read from kafka:{n_snapshots_leidos_kafka}")
                                    #LOGGER.info (f"Tiempos proceso en intervalos:{l_t_proc_int}")
                                    LOGGER.info ("-------------------------------------------------------\n")
                                    
                                    # Log de estadisticas de snapshots descolocados
                                    l_counter_snapshots.append(n_snapshots_int)
                                    l_counter_late.append(n_snapshots_late_int)
                                    l_counter_rejected.append(n_snapshots_rejected_int)
                
                                    num_rafaga_snapshots_futuro=0
                                    n_snapshots_int = 0
                                    n_snapshots_raw_int = 0
                                    n_snapshots_tot-= num_pendientes
                                    n_snapshots_late_int= 0
                                    n_snapshots_rejected_int= 0
                                    n_snapshots_dentro_int= 0
                                    interval_counter += 1
                                    
                                    
                                    # 
                                    #time_to_plot = time_interval_end - timedelta(hours=2)

                                    if HORARIO_VERANO:
                                        # Horario de verano
                                        time_to_plot = time_interval_end - timedelta(hours=2)
                                    else:
                                        # Horario de invierno
                                        time_to_plot = time_interval_end - timedelta(hours=1)

                    
                                    ml_avg_confidence_per_class = self.get_ml_avg_confidence_per_class(ml_confidence_all)
                                    
                                    #ESCRITURA EN EL INFLUX: DATOS DEL INTERVALO POR CLASES y DATOS GLOBALES
                                    # ALB DEBUG
                                    if interval_counter > 0:
                                        self.escribir_influx (time_to_plot, tot_bytes_all,tot_packets_all,ml_avg_confidence_per_class,security_status,unique_hh_stats)
                                    #self.escribir_influx (time_to_plot, snapshots_per_class_int,tot_packets_all,ml_avg_confidence_per_class,security_status,unique_hh_stats)
                                    
                                    #RESETEAR CONTADORES PARA NUEVO INTERVALO
                                    
                                    time_interval_start = time_interval_end
                                    time_interval_end = time_interval_start + timedelta(seconds=self.monitored_time_interval_agg)
                                    #tiempos relativos intervalo a partir de t=0
                                    interval_start_relative = interval_end_relative
                                    interval_end_relative = interval_start_relative + self.monitored_time_interval_agg
                    
                                    ml_confidence_all = {i: defaultdict(list) for i in self.label_correspondence.values()}
                                    tot_bytes_all = {i: 0 for i in self.label_correspondence.values()}
                                    tot_packets_all = {i: 0 for i in self.label_correspondence.values()}
                                    snapshots_per_class_int = {i: 0 for i in self.label_correspondence.values()}
                                    unique_hh_stats = {"hh_connection": set(), "hh_source": set(), "hh_target": set(), "attack_connection": set(), "ddos_attacker": set(), "ddos_target": set()}
                                    security_status = 0.0
                                    num_malign_interval = 0
                                    start_aggregating = time.time()

                                    # Acumula debugging por intervalo y resetea contadores
                                    record_profiling = MetricsRecord (interval_counter=interval_counter,
                                                        t_poll=t_poll, n_polls=n_polls,
                                                        t_not_mensajes=t_not_mensajes, n_not_mensajes=n_not_mensajes,
                                                        t_not_pendientes_procesadas=t_not_pendientes_procesadas, n_not_pendientes_procesadas=n_not_pendientes_procesadas,
                                                        t_snap_fuera_int=t_snap_fuera_int, n_snap_fuera_int=n_snap_fuera_int,
                                                        t_snap_dentro_int=t_snap_dentro_int, n_snap_dentro_int=n_snap_dentro_int
                                                    )
                                    l_record_profiling.append (record_profiling)
                                    #
                                    t_poll=0.0; n_polls=0
                                    t_not_mensajes= 0.0; n_not_mensajes=0
                                    t_not_pendientes_procesadas=0.0; n_not_pendientes_procesadas=0
                                    t_snap_fuera_int=0.0; n_snap_fuera_int=0
                                    t_snap_dentro_int=0.0; n_snap_dentro_int=0

                                    if interval_counter == LAST_INTERVAL:
                                        if self.crear_csv:
                                            file_snap.close()
                                        self.write_in_csv_interval(NOMBRE_ARCHIVO_POR_INTERVALO, l_counter_late, l_counter_rejected, l_counter_snapshots)
                                        self.write_in_csv_snapshots(NOMBRE_ARCHIVO_POR_SNAPSHOT, lt_nfstream_inference, lt_inference_detector, lt_detector_monitoring, lt_total_delay)      
                                        #LOGGER.info (f"l_record_profiling : {l_record_profiling}")
                                        with open('metrics_records.pkl', 'wb') as f:
                                            pickle.dump(l_record_profiling, f)
                                        return
                    
                                else:
                                    # No hemos creado intervalo, seguimos recorriendo labels
                                    # Actualizamos el indice para procesar el siguiente elemento
                                    i+=1
                                t21=time.time()
                                t_snap_fuera_int+=t21-t11
                                n_snap_fuera_int+=1
                            
                            else : 
                                n_snap_dentro_w+=1

                                if self.crear_csv:
                                    m=metadatas[i]
                                    conn_id = f"{m['src_ip']}*{m['dst_ip']}*{m['src_port']}*{m['dst_port']}*{m['protocol']}"
                                    snap= f"{m['timestamp']}*{adjusted_time}*{m['timestamp_nfstream']}*{m['timestamp_ai_mon_get_kafka']}*{m['timestamp_inference']}*{m['timestamp_ai_mon_put_kafka']}*{m['timestamp_det_get_kafka']}*{m['timestamp_detector']}*{m['timestamp_det_put_kafka']}*{t_get_kafka}*{time.time()}*{conn_id}"
                                    file_snap.write (snap+"\n")
                                
                                t12=time.time()
                                # Esta dentro del intervalo o es anterior 
                                #if (interval_counter ==INT_DEBUG): LOGGER.info ("XXXX snapshot dentro intervalo o anterior.")
                                t_ultimo_snap_intervalo= datetime.now()
                                num_rafaga_snapshots_futuro=0
                                #LOGGER.info (f"XXXX racha de snapshots fuera DESPUES de RESET. num_rafaga_snapshots_futuro:{num_rafaga_snapshots_futuro}  ")
                                
                                if adjusted_time < (time_interval_start - timedelta(seconds=5)):
                                    # Llega demasiado tarde (< t-1), no se procesa
                                    n_snapshots_late_int += 1
                                    n_snapshots_rejected_int +=1
                                    n_snapshots_late_tot += 1
                                    n_snapshots_rejected_tot +=1
                                    #if (interval_counter == INT_DEBUG): 
                                    #LOGGER.info(f"XXXX Snapshot LLEGA DEMASIADO TARDE: adjusted_time {adjusted_time} (time interval: {time_interval_start}, {time_interval_end}). n_snapshots_late_int:{n_snapshots_late_int}. n_snapshots_rejected_int:{n_snapshots_rejected_int}")
                                    #LOGGER.info (f"")
                                    #LOGGER.info ("adjusted_time = timedelta(seconds=metadata['timestamp'] - first_timestamp_real) + first_timestamp_nfstream")
                                else:
                                    # Se procesa el snapshot
                                    n_snapshots_dentro_int+=1
                                    n_snapshots_dentro_tot+=1      
                                    if adjusted_time < time_interval_start:
                                        if (interval_counter == INT_DEBUG): LOGGER.info ("XXXX Snapshot llega tarde pero se procesa")
                                        # Llega tarde en t-1, pero lo procesamos en t (son contiguos)
                                        n_snapshots_late_int += 1
                                        n_snapshots_late_tot += 1
                                        #LOGGER.info(f"SNAPSHOT LLEGA TARDE:{adjusted_time} ({time_interval_start}, {time_interval_end}). n_snapshots_late_int:{n_snapshots_late_int}. n_snapshots_rejected_int:{n_snapshots_rejected_int}")
                                    #else: 
                                        #if (interval_counter == INT_DEBUG): LOGGER.info ("XXXX Snapshot llega dentro de intervalo")
                                                    
                                    # CALCULAR ESTADÍSTICAS PARA CADA SNAPSHOT
                                      
                                    start_calculating = time.time()
                                    flow_bytes = int(metadata["flow_bytes"])
                                    flow_packets = int(metadata["flow_pkts"])
                                    #conn_id = (metadata['src_ip'], metadata['dst_ip'], metadata['src_port'], metadata['dst_port'], metadata['first'])
                                    conn_id = (metadata['src_ip'], metadata['dst_ip'], metadata['src_port'], metadata['dst_port'])
                                    
                                    if flow_packets > packets_dict[conn_id]:
                                        # Pueden venir desordenados
                                        tot_delta_bytes_all = flow_bytes - bytes_dict[conn_id]
                                        tot_delta_packets_all = flow_packets - packets_dict[conn_id]
                                        #DEBUG
                                        #LOGGER.info (f"interval_counter:{interval_counter} label:{label} tot_delta_bytes_all:{tot_delta_bytes_all}")
                                        #LOGGER.info (f"interval_counter:{interval_counter} label:{label} tot_delta_packets_all:{tot_delta_packets_all}") 
                                        tot_bytes_all[label] += tot_delta_bytes_all
                                        tot_packets_all[label] += tot_delta_packets_all
                                        
                                        bytes_dict[conn_id] = flow_bytes
                                        packets_dict[conn_id] = flow_packets
                                        
                                    snapshots_per_class[label] +=1
                                    snapshots_per_class_int[label] +=1
                                    
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
                                            #security_status = 1
                                            num_malign_interval+=1
                                            
                                    security_status = float(num_malign_interval / n_snapshots_dentro_int) if n_snapshots_dentro_int >0 else 0
                                    #        if interval_counter > 90 :
                                    #            LOGGER.info (f"security_status: {security_status} num_malign_interval:{num_malign_interval}, n_snapshots_dentro_int:{n_snapshots_dentro_int}")
                                                                
                                    ml_confidence_all[label][conn_id].append(metadata['ml_confidence'])
                                    
                                    #if (interval_counter == INT_DEBUG): LOGGER.info("Tiempo tardado en cálculo de métricas: %s", time.time() - start_calculating)

                                t22=time.time()
                                t_snap_dentro_int+=t22-t12
                                n_snap_dentro_int+=1
                    
                                # Actualizamos el indice para procesar el siguiente elemento
                                i+=1
                                
                            # end if/else adjusted_time >= time_interval_end: (Esta fuera o dentro del intervalo)
                                
                        #end while i < len(labels):
                        t2_while=time.time()
                        t_while=t2_while-t1_while
                        if (interval_counter == INT_DEBUG): LOGGER.info (f"fin while i < len(labels). i:{i}, t_while:{t_while}, t_while_per_label:{round(t_while/(n_snap_dentro_w+n_snap_next_w),7)}. n_snap_dentro_w:{n_snap_dentro_w}, n_snap_next_w:{n_snap_next_w}. len(labels_pendientes):{len(labels_pendientes)}")
                    #end for consumer_record in a_consumer_records: 
                    if (interval_counter == INT_DEBUG): LOGGER.info (f"fin for consumer_record in a_consumer_records. len(labels_pendientes):{len(labels_pendientes)}")
                #end for mensaje in mensajes:
                t2_for=time.time()
                t2_if_else=time.time()
                if (interval_counter == INT_DEBUG): LOGGER.info (f"fin 'for mensaje in mensajes'. tiempo:{t2_for-t1_for}. len(labels_pendientes):{len(labels_pendientes)}")
                if (interval_counter == INT_DEBUG): LOGGER.info (f"t_ if not mensajes else:{t2_if_else-t1_if_else}")
                
            #end  if/else not mensajes:   
            t2_while_main=time.time()
            if (interval_counter == INT_DEBUG):LOGGER.info (f"t iteracion dentro while:{t2_while_main-t1_while_main}")
        #end while (True)
    

            
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
    
    #profiler = cProfile.Profile()
    #profiler.enable()
    
    # Call the function you want to profile
    telemetry.start_monitoring()
    
    #profiler.disable()
    #stats = pstats.Stats(profiler)
    #stats.sort_stats('cumulative').print_stats(100)

    #DEbugging para mantener vivo el contenedor y poder copiar los ficheros de debugging del contenedor
    while (True) :
        time.sleep(INT_DEBUG)
    


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
