FROM apache/spark-py:latest

USER root

# Cài đặt các công cụ hệ thống nếu cần (cho psycopg2)
RUN apt-get update && apt-get install -y gcc libpq-dev && apt-get clean

WORKDIR /opt/spark/work-dir

# Copy và cài đặt requirements
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy script xử lý
COPY process_weather.py .

USER spark