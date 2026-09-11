FROM python:3.12-slim-bookworm

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    KAFKA_URL=kafka:9092 \
    CONSUMER_TOPIC=inference_probs \
    PRODUCER_TOPIC=predicted_labels \
    CONSUMER_CLIENT_ID=ai-detector-consumer \
    PRODUCER_CLIENT_ID=ai-detector-producer \
    CONSUMER_GROUP_ID=ai-detector \
    DETECTOR_CSV_DIR=/app/output_csv

WORKDIR /app

COPY ai-detector/files/requirements.txt ./requirements.txt
RUN python -m pip install --no-cache-dir --upgrade pip \
    && python -m pip install --no-cache-dir -r requirements.txt

COPY tc35_contract/ ./tc35_contract/
COPY ai-detector/files/ai_detector.py ./

RUN useradd --create-home --uid 10001 tc35 \
    && mkdir -p /app/output_csv \
    && chown -R tc35:tc35 /app
USER tc35

CMD ["python", "./ai_detector.py"]
