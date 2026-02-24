


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
                            self.write_in_csv_interval(NOMBRE_ARCHIVO_POR_INTERVALO, l_counter_late, l_counter_rejected, l_counter_snapshots)
                            self.write_in_csv_snapshots(NOMBRE_ARCHIVO_POR_SNAPSHOT, lt_nfstream_inference, lt_inference_detector, lt_detector_monitoring, lt_total_delay)                        
                        
                        time_to_plot = time_interval_end - timedelta(hours=2)
                        ml_avg_confidence_per_class = self.get_ml_avg_confidence_per_class(ml_confidence_all)
                        
                        #ESCRITURA EN EL INFLUX: DATOS DEL INTERVALO POR CLASES y DATOS GLOBALES
                        escribir_influx (time_to_plot, tot_bytes_all,tot_packets_all,ml_avg_confidence_per_class,security_status,unique_hh_stats)
                        
                        #RESETEAR CONTADORES PARA NUEVO INTERVALO
                        
                        time_interval_start = time_interval_end
                        time_interval_end = time_interval_start + timedelta(seconds=self.monitored_time_interval_agg)
                        interval_start_relative = interval_end_relative
                        interval_end_relative = interval_start_relative + self.monitored_time_interval_agg

                        ml_confidence_all = {i: defaultdict(list) for i in self.label_correspondence.values()}
                        tot_bytes_all = {i: 0 for i in self.label_correspondence.values()}
                        tot_packets_all = {i: 0 for i in self.label_correspondence.values()}
                        unique_hh_stats = {"hh_connection": set(), "hh_source": set(), "hh_target": set(), "attack_connection": set(), "ddos_attacker": set(), "ddos_target": set()}
                        security_status = 0
                        start_aggregating = time.time()
