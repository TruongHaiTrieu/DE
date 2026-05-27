FROM apache/spark-py:latest

USER root

RUN apt-get update && apt-get install -y gcc libpq-dev && apt-get clean

WORKDIR /opt/spark/work-dir

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY weather_consumer.py .

USER spark