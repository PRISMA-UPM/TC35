FROM python:3.12-slim-bookworm

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    CATALOG_URL=http://ai-repository:9200 \
    CATALOG_INDEX=models \
    MODELS_PATH=/app/models/rev4

WORKDIR /app

COPY model-upload/files/requirements.txt ./requirements.txt
RUN python -m pip install --no-cache-dir --upgrade pip \
    && python -m pip install --no-cache-dir -r requirements.txt

COPY model-upload/files/model_upload.py ./
COPY model-upload/files/models/ ./models/

RUN useradd --create-home --uid 10001 tc35 \
    && chown -R tc35:tc35 /app
USER tc35

CMD ["python", "./model_upload.py"]
