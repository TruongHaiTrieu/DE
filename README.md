#**🌦️ End-to-End Weather Data Pipeline (Real-time ETL)**

Hệ thống thu thập dữ liệu thời tiết thời gian thực từ OpenWeather API, xử lý qua Kafka & Spark Streaming và lưu trữ vào PostgreSQL.

<img width="1080" height="341" alt="{9C6C25FD-4630-4B9E-A95F-F613E7322833}" src="https://github.com/user-attachments/assets/d0823b18-76a0-48fa-ae9f-ef422965dcbf" />


## **🛠 1. Quy trình xây dựng (Step-by-Step)**

### **Bước 1**: Khởi tạo dự án và Môi trường ảo (venv)

Mở terminal tại thư mục gốc của dự án:

```powershell 
python -m venv venv
# Kích hoạt venv
# Windows:
.\venv\Scripts\activate
# Linux/Mac:
source venv/bin/activate
```

### **Bước 2**: Thiết lập Docker Compose & .env

1. Tạo file `docker-compose.yml`: Định nghĩa các dịch vụ Kraft, Kafka-ui, Spark, Postgres, grafana và pgAdmin.

2. Tạo file `.env`: Lưu trữ các thông tin nhạy cảm.

3. Tạo `.env.example`: Copy từ .env nhưng để trống giá trị để push lên Git.

### **Bước 3**: Phát triển thư mục Producer

1. Tạo thư mục: `mkdir producer -> cd producer.`

2. Viết Script: Tạo file `weather_producer.py` để lấy dữ liệu từ API và gửi vào Kafka.

3. Cài đặt thư viện:

```Bash
pip install kafka-python requests python-dotenv
```
4. Tạo Requirements:

```Bash
pip freeze > requirements.txt
```

5. Viết Dockerfile: Tạo file `Dockerfile` trong thư mục `producer` để container hóa script này.

### **Bước 4**: Phát triển thư mục Spark Processor
1. Tạo thư mục: `mkdir spark -> cd spark`.

2. Viết Script ETL: Tạo file `process_weather.py` sử dụng `foreachBatch` và `checkpointLocation`.

3. Viết Dockerfile: Sử dụng image  `apache/spark-py:latest` làm base.

4. Tạo Checkpoint: Tạo thư mục con `checkpoints/weather_stream` để lưu trạng thái xử lý.

## **🚀 2. Cách triển khai hệ thống**
Tại thư mục gốc, chạy lệnh duy nhất:

```Bash
docker-compose up -d --build
```

## **📊 3. Hướng dẫn giám sát (Monitoring) Step-by-Step**

Để kiểm tra xem hệ thống có chạy đúng không, hãy truy cập theo thứ tự sau:

### **Bước 1**: Giám sát luồng tin nhắn (Kafka UI)

- **Địa chỉ**: http://localhost:8080

- **Cách kiểm tra**: Vào mục **Topics** -> Chọn **weather_topic** -> Chọn tab **Messages**. Nếu thấy dữ liệu JSON đổ về liên tục, nghĩa là Producer đã thành công.

<img width="1769" height="959" alt="kafka" src="https://github.com/user-attachments/assets/3246e5b6-2021-4161-94ef-5c63bb49b57a" />

### **Bước 2**: Giám sát xử lý dữ liệu (Spark UI)

- **Địa chỉ**: http://localhost:8081 (Master UI) hoặc cổng `4040` khi job đang chạy.

- **Cách kiểm tra**: Xem mục **Running Applications**. Nếu thấy `job WeatherStream` đang chạy và không có lỗi (Failed), nghĩa là Spark đang xử lý các Batch.

<img width="1785" height="957" alt="spark" src="https://github.com/user-attachments/assets/afc2091e-2546-484b-9d26-5e2506734138" />


### **Bước 3**: Kiểm tra kho lưu trữ (pgAdmin)
- **Địa chỉ**: http://localhost:5050

- Cách thiết lập lần đầu:

  1.Đăng nhập bằng email/pass trong docker-compose (mặc định: `admin@admin.com `/ `admin`).

  2.**Add New Sever** -> 
    - Tab **General**: Name là `Weather_DB`
    - Tab **Connection**: Host là `postgres`, Port `5432`, Maintenance database là `weather_db`,Username/Pass là thông tin trong `.env`.

- Xem dữ liệu: Vào đúng **Database** -> **Schemas** -> **public** -> **Tables** -> Chuột phải bảng **weather_data** -> **View Data** -> **All Rows**.

<img width="1792" height="961" alt="pd" src="https://github.com/user-attachments/assets/cf458d32-21ad-4ed3-8e12-65dab780bfbc" />

### **Bước 4** Dashboard trực quan hóa dữ liệu (grafana) 

- **Địa chỉ**: http://localhost:3000

- Cách thiết lập lần đầu:

  1.Đăng nhập bằng email/pass trong docker-compose (mặc định: `admin`/ `admin`). Update your password : skip 

  2.**Connections** -> **Data sources** -> **search**: postgres -> **grafana-postgresql-datasource**\
    - **Connection**:  Host URL là `postgres:5432`, Database name là `weather_db`
    - **Authentication**:  Username/Pass là thông tin trong `.env`, TLS/SSL Mode là `disable`
  3.**Savs & test**
  . **Dashboards** -> **Create dashboard** -> **Add visualization** -> **Select**: grafana-postgresql-datasource

<img width="1784" height="953" alt="image" src="https://github.com/user-attachments/assets/90bac50f-9659-49f7-b7b6-ce2e8a2c5b5f" />

## **🏗 4. Cấu trúc thư mục dự án (Project Structure)**
Dựa trên cấu trúc đang có:

<img width="386" height="505" alt="st" src="https://github.com/user-attachments/assets/6ebbdc9a-bbec-4ace-8e9f-6a9467fc72a7" />

- `producer/`: Chứa mã nguồn thu thập dữ liệu.

- `spark/`: Chứa mã nguồn xử lý luồng (Stream Processing) và checkpoints.

- `docker-compose.yml`: "Nhạc trưởng" kết nối tất cả các thành phần.

- `.env`: Quản lý bí mật (Secrets).

- `.gitignore`: Loại bỏ venv và .env khỏi Git.

## **💡 Lưu ý quan trọng**
**Checkpoint**: Nếu bạn sửa Schema của dữ liệu, hãy xóa thư mục `spark/checkpoints` trước khi khởi động lại để tránh lỗi xung đột metadata.

**Volume**: Thư mục `spark` được mount vào container để đảm bảo code và checkpoint không bị mất khi container bị tắt.
