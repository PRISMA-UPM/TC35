FROM python:3.8.10

ENV CATALOG_URL="http://localhost:9200"

ENV CATALOG_INDEX="models"

ENV DATA_PATH="./data/"

ENV PORT=5050

RUN apt-get update -y

RUN python3 -m pip install --no-cache-dir --upgrade pip

RUN python3 -m pip install --upgrade setuptools wheel pip-tools

WORKDIR /usr/app/src

COPY files/requirements.txt ./

RUN python3 -m pip install -r requirements.txt

COPY files/ai_training.py ./

RUN mkdir -p data models

ENV TZ="Europe/Madrid"

EXPOSE 5050

CMD python3 ./ai_training.py --catalog_url=$CATALOG_URL --catalog_index=$CATALOG_INDEX --data_path=$DATA_PATH --port=$PORT
