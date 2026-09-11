FROM python:3.10.18-slim-bookworm

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    CATALOG_URL=http://ai-repository:9200 \
    CATALOG_INDEX=models \
    DATA_PATH=/app/data \
    MODELS_OUT_PATH=/app/models \
    PORT=5050

WORKDIR /app

COPY ai-training/files/requirements.txt ./requirements.txt
RUN python -m pip install --no-cache-dir --upgrade pip setuptools wheel \
    && python -m pip install --no-cache-dir -r requirements.txt

COPY tc35_contract/ ./tc35_contract/
COPY ai-training/files/ai_training.py ./

RUN useradd --create-home --uid 10001 tc35 \
    && mkdir -p /app/data /app/models \
    && chown -R tc35:tc35 /app
USER tc35

EXPOSE 5050
CMD ["python", "./ai_training.py"]
