FROM python:3.9-slim

# Cài đặt các thư viện hệ thống cần thiết để build các gói python (nếu có)
RUN apt-get update && apt-get install -y \
    gcc \
    python3-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
# Nâng cấp pip trước khi cài đặt
RUN pip install --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

COPY weather_producer.py .

CMD ["python", "weather_producer.py"]