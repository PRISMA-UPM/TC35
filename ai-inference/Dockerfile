FROM python:3.10.18-slim-bookworm

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    KAFKA_URL=kafka:9092 \
    CATALOG_URL=http://ai-repository:9200 \
    CONSUMER_TOPIC=inference_data \
    PRODUCER_TOPIC=inference_probs \
    CONSUMER_CLIENT_ID=ai-inference-consumer \
    PRODUCER_CLIENT_ID=ai-inference-producer \
    CONSUMER_GROUP_ID=ai-inference \
    CATALOG_MODELS_INDEX=models \
    INFERENCE_BATCH_SIZE=900 \
    INFERENCE_IDLE_POLL_SECONDS=0.001 \
    MODELS_PATH=/app/models

WORKDIR /app

COPY ai-inference/files/requirements.txt ./requirements.txt
RUN python -m pip install --no-cache-dir --upgrade pip setuptools wheel \
    && python -m pip install --no-cache-dir -r requirements.txt

COPY tc35_contract/ ./tc35_contract/
COPY ai-inference/files/ai_inference.py ./

RUN useradd --create-home --uid 10001 tc35 \
    && mkdir -p /app/models \
    && chown -R tc35:tc35 /app

USER tc35

CMD ["python", "./ai_inference.py"]
