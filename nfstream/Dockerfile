FROM python:3.10.18-slim-bookworm

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    KAFKA_URL=kafka:9092 \
    PRODUCER_TOPIC=inference_data \
    PRODUCER_CLIENT_ID=nfstream-producer \
    T_LIM=0.5 \
    SAMPLING_RATE=1.0 \
    NFSTREAM_METERS=6 \
    NFSTREAM_REQUIRE_EXACT_METERS=true \
    IDLE_TIMEOUT=120 \
    KAFKA_MAX_PENDING_SENDS=1000 \
    PCAP_FRENO_MAX_LEAD_SECONDS=0.25 \
    KAFKA_BATCH_SIZE=32768 \
    KAFKA_LINGER_MS=10 \
    KAFKA_COMPRESSION_TYPE=gzip \
    KAFKA_FLUSH_PACKET_INTERVAL=900 \
    KAFKA_CLEANUP_FLUSH_FLOW_INTERVAL=300 \
    KAFKA_ACKS=1 \
    KAFKA_RETRIES=0

RUN apt-get update \
    && apt-get install --yes --no-install-recommends libpcap0.8 libpcap0.8-dev libgomp1 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY nfstream/requirements.txt ./requirements.txt
RUN python -m pip install --no-cache-dir --upgrade pip setuptools wheel \
    && python -m pip install --no-cache-dir -r requirements.txt

COPY tc35_contract/ ./tc35_contract/
COPY nfstream/nfmod/ ./nfmod/
COPY nfstream/files/nfstream_kafka.py ./

RUN useradd --create-home --uid 10001 tc35
USER tc35

CMD ["python", "./nfstream_kafka.py"]
