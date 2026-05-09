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

