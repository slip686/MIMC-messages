FROM python:3.8.3-slim 

RUN apt-get update \
    && apt-get -y install libpq-dev gcc \
    && pip install psycopg2

WORKDIR /db_messages_broker_script
COPY db_message_broker_script.py ./
COPY config.py ./
COPY requirements.txt ./
RUN pip install -r requirements.txt
CMD ["python3", "./db_message_broker_script.py"]
