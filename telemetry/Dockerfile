FROM python:3.8.10

ENV KAFKA_URL="localhost:9094"

ENV CONSUMER_TOPIC="predicted_labels"

ENV CONSUMER_CLIENT_ID="telemetry-consumer"

ENV CONSUMER_GROUP_ID="telemetry"

ENV INFLUX_URL="http://localhost:8086"

ENV INFLUX_BUCKET="monitoring"

ENV INFLUX_ORG="across"

ENV INFLUX_TOKEN="AU3tZWhRauEKhrcF1R6KbT8n"

ENV INFLUX_USERNAME="admin"

ENV INFLUX_PASSWORD="admin-lab"

ENV TIME_INTERVAL=5

ENV MAX_POLL_RECORDS=500

RUN apt-get update -y

RUN python3 -m pip install --no-cache-dir --upgrade pip

RUN python3 -m pip install --upgrade setuptools wheel pip-tools

WORKDIR /usr/app/src

COPY files/requirements.txt ./

RUN python3 -m pip install -r requirements.txt

RUN python3 -m pip install dill

COPY files/telemetry_prueba_intervalos.py ./

ENV TZ="Europe/Madrid"

CMD python3 ./telemetry_prueba_intervalos.py --kafka_url=$KAFKA_URL --consumer_topic=$CONSUMER_TOPIC --consumer_client_id=$CONSUMER_CLIENT_ID --consumer_group_id=$CONSUMER_GROUP_ID --influx_url=$INFLUX_URL --influx_bucket=$INFLUX_BUCKET --influx_org=$INFLUX_ORG --influx_token=$INFLUX_TOKEN --time_interval=$TIME_INTERVAL --max_poll_records=$MAX_POLL_RECORDS
