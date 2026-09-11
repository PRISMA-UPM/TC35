FROM python:3.12-slim-bookworm

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    KAFKA_URL=kafka:9092 \
    CONSUMER_TOPIC=predicted_labels \
    CONSUMER_CLIENT_ID=telemetry-consumer \
    CONSUMER_GROUP_ID=telemetry \
    MAX_POLL_RECORDS=500 \
    INFLUX_URL=http://influxdb:8086 \
    INFLUX_BUCKET=monitoring \
    INFLUX_ORG=tc35 \
    TIME_INTERVAL=5 \
    ALLOWED_LATENESS_SECONDS=5 \
    IDLE_FLUSH_SECONDS=1 \
    REBASE_EVENT_TIME=true \
    MONITORED_LABELS=normal_traffic,benign_heavy_hitter,malign_heavy_hitter \
    BENIGN_HH_LABELS=benign_heavy_hitter \
    MALIGN_LABELS=malign_heavy_hitter \
    INTERVAL_CSV_PATH=/app/output/retrasos_intervalo.csv \
    LATENCY_CSV_PATH=/app/output/retrasos_snapshot.csv

WORKDIR /app

COPY intelligent-telemetry/files/requirements.txt ./requirements.txt
RUN python -m pip install --no-cache-dir --upgrade pip \
    && python -m pip install --no-cache-dir -r requirements.txt

COPY tc35_contract/ ./tc35_contract/
COPY intelligent-telemetry/files/telemetry.py ./

RUN useradd --create-home --uid 10001 tc35 \
    && mkdir -p /app/output \
    && chown -R tc35:tc35 /app
USER tc35

CMD ["python", "./telemetry.py"]
