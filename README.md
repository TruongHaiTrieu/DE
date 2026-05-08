# 📄 Weather Data Streaming Pipeline
Hệ thống xử lý dữ liệu thời tiết thời gian thực (Real-time) sử dụng mô hình kiến trúc Modern Data Stack. Dữ liệu được thu thập từ OpenWeatherMap API, truyền tải qua Kafka, xử lý bằng Spark Streaming và lưu trữ vào PostgreSQL để phân tích.

# 🏗 Kiến trúc hệ thống
Hệ thống bao gồm các thành phần chính:
- Producer (Python): Thu thập dữ liệu từ API và gửi vào Kafka Topic.Apache Kafka (KRaft mode): Hệ thống hàng đợi tin nhắn chịu lỗi cao.Apache Spark 
- (Structured Streaming): Xử lý, làm sạch và ép kiểu dữ liệu theo thời gian thực.
- PostgreSQL: Cơ sở dữ liệu quan hệ dùng để lưu trữ dữ liệu đã qua xử lý.
- pgAdmin & Kafka-UI: Giao diện trực quan để giám sát dữ liệu và hệ thống.

# 🚀 Hướng dẫn triển khai
1. Yêu cầu hệ thống 
- Đã cài đặt Docker và Docker Compose.
- Python 3.x (để chạy Producer).

2. Khởi động môi trường Docker

Mở terminal tại thư mục gốc của dự án và chạy lệnh:

```
docker-compose up -d
```

*Lệnh này sẽ khởi chạy Kafka cluster, Spark Master/Worker, PostgreSQL và các công cụ giám sát.3. Cài đặt thư viện Python cho ProducerBashpip install kafka-python requests*

3. Chạy các thành phần của Pipeline

**Bước A: Chạy Weather Producer**

```
python producer/weather_producer.py
```

**Bước B: Chạy Spark Structured Streaming**
Gửi job vào Spark cluster để bắt đầu xử lý:

```
docker exec -it spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0,org.postgresql:postgresql:42.6.0 \
  /opt/spark/work-dir/process_weather.py
```

# 📊 Giám sát và Kiểm tra dữ liệu


|Công cụ|Địa chỉ (URL)|Chức năng|
|---|---|---|
|Kafka-UIUI|http://localhost:8080UI|Xem các tin nhắn đang chảy trong Kafka Topic|
|Spark MasterUI|http://localhost:8081UI|Theo dõi trạng thái các Jobs và Workers|
|pgAdminUI|http://localhost:5050UI|Giao diện quản lý Database (User: admin@admin.com)|

Kết nối Database trong pgAdmin/DBeaver:
- Host: localhost (nếu dùng DBeaver) hoặc postgres (nếu dùng pgAdmin trong Docker)
- Port: 5432
- Database: weather_db
- User: user | Password: 123456

# 🛠 Cấu trúc thư mụcPlaintext.
├── docker-compose.yml        # Định nghĩa các dịch vụ Docker<br>
├── producer/<br>
$\hspace{0.3cm}$   └── weather_producer.py   # Script lấy dữ liệu từ API gửi vào Kafka<br>
├── spark/<br>
$\hspace{0.3cm}$  └── process_weather.py    # Script Spark Streaming xử lý dữ liệu<br>
└── README.md                 # Tài liệu hướng dẫn<br>

