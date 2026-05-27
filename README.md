#**🌦️ End-to-End Weather Data Pipeline (Real-time ETL)**


![Python](https://img.shields.io/badge/Python-3.10%2F3.11-blue?logo=python)
![Kafka](https://img.shields.io/badge/Apache%20Kafka-KRaft%207.5.0-write?logo=apachekafka)
![PySpark](https://img.shields.io/badge/PySpark-3.5-orange?style=flat&logo=apache-spark&logoColor)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15-blue?style=flat-square&logo=postgresql&logoColor=white)
![Grafana](https://img.shields.io/badge/Grafana-v11-orange?style=flat-square&logo=grafana&logoColor)
![Docker](https://img.shields.io/badge/Docker-v27+-blue?style=flat-square&logo=docker&logoColor)


Hệ thống xử lý luồng dữ liệu thời tiết thời gian thực (Real-time Data Pipeline): Tự động khai thác dữ liệu từ OpenWeather API, truyền tải qua nền tảng trung gian Apache Kafka, xử lý phân tán bằng PySpark Streaming trước khi lưu trữ định hình vào cơ sở dữ liệu PostgreSQL để phục vụ trực quan hóa trên Grafana. To Toàn bộ hệ thống được container hóa và triển khai đồng bộ bằng Docker.

## **🖼️ Sơ đồ hệ thống**

### Data Engineering Pipeline

<img width="1209" height="467" alt="image" src="https://github.com/user-attachments/assets/6c863e6b-06eb-4308-9d0a-c86b1c131c89" />


## **🏗️ Cấu trúc thư mục**


```
Weather-data-pipeline/
├── .env                  # Lưu trữ các biến môi trường cấu hình hệ thống (Secrets, API Keys)
├── .env.example          # File mẫu cấu hình biến môi trường (Không chứa thông tin nhạy cảm)
├── .gitignore            # Khai báo các file/thư mục không đưa lên Git (như .env, checkpoints)
├── docker-compose.yml    # File điều phối, khởi tạo toàn bộ hạ tầng (Kafka, Spark, Postgres, Grafana)
│
├── producer/             # Thành phần đảm nhiệm thu thập dữ liệu
│   ├── Dockerfile        # Dockerfile đóng gói môi trường cho Python Producer
│   ├── requirements.txt  # Thư viện Python cần thiết (requests, kafka-python,...)
│   └── weather_producer.py # Script gọi OpenWeather API và đẩy dữ liệu (Publish) vào Kafka
│
└── consumer/             # Thành phần xử lý dữ liệu thời gian thực
    ├── Dockerfile        # Dockerfile build môi trường chạy PySpark & Java JVM
    ├── java_opts.txt     # Cấu hình tối ưu hóa tài nguyên cho Java Virtual Machine (JVM)
    ├── consumer_weather.py # Script xử lý luồng dữ liệu (Spark Streaming), gom cụm và lưu vào Postgres
    ├── requirements.txt  # Thư viện Python hỗ trợ cho Spark (pyspark, psycopg2,...)
    └── checkpoints/      # Thư mục lưu trạng thái stream dữ liệu (Giúp hệ thống tự phục hồi khi lỗi)
        └── weather_stream/
            ├── .metadata.crc
            ├── metadata
            └── commits/

```
## **🎯 Lợi ích của các công nghệ sử dụng**

![Kafka](https://img.shields.io/badge/Apache%20Kafka-KRaft%207.5.0-write?logo=apachekafka)<br>
Trục điều phối dữ liệu (Message Broker)

- **Chống nghẽn dữ liệu (Backpressure)**: Đóng vai trò là vùng đệm lưu trữ tạm thời, giúp hệ thống không bị quá tải khi tốc độ gọi API OpenWeather nhanh hơn tốc độ xử lý của database.

- **Tách biệt kiến trúc (Decoupling)**: Giúp tách rời hoàn toàn quá trình thu thập dữ liệu (Producer) và quá trình xử lý dữ liệu (Consumer), dễ dàng bảo trì độc lập.

![PySpark](https://img.shields.io/badge/PySpark-3.5-orange?style=flat&logo=apache-spark&logoColor)<br>
Động cơ xử lý luồng phân tán

- **Sẵn sàng mở rộng (Scalability)**: Chuẩn hóa kiến trúc Big Data. Định hình hệ thống sẵn sàng xử lý hàng triệu thiết bị cảm biến hoặc trạm thời tiết gửi về cùng lúc trong tương lai.

- **Cơ chế chịu lỗi tuyệt đối (Fault-tolerance)**: Sử dụng Checkpoints giúp lưu lại vị trí luồng dữ liệu. Nếu hệ thống sập đột ngột, Spark sẽ tự động chạy tiếp từ điểm dừng mà không làm mất hay trùng lặp dữ liệu.

![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15-blue?style=flat-square&logo=postgresql&logoColor=white)<br>
Cơ sở dữ liệu lưu trữ cấu trúc

- **Tối ưu dữ liệu định hình**: Dữ liệu thời tiết có cấu trúc cố định và tường minh, lưu trữ dạng bảng (Relational DB) giúp tiết kiệm dung lượng và tối ưu hóa hiệu năng.

- **Truy vấn phân tích mạnh mẽ**: Hỗ trợ xử lý dữ liệu chuỗi thời gian (Timestamp) và các hàm gộp toán học (AVG, MIN, MAX) để phục vụ báo cáo.

![Grafana](https://img.shields.io/badge/Grafana-v11-orange?style=flat-square&logo=grafana&logoColor)<br>
Giao diện trực quan hóa thời gian thực

- Tự động làm mới (Auto-refresh): Tối ưu riêng cho dữ liệu dạng chuỗi thời gian, tự động cập nhật biểu đồ theo giây mà không cần tải lại trang.

- Phát triển nhanh chóng: Thay vì tốn hàng tuần code Frontend, Grafana giúp tạo Dashboard theo dõi biến động nhiệt độ/độ ẩm chuẩn doanh nghiệp chỉ qua vài thao tác cấu hình.

![Docker](https://img.shields.io/badge/Docker-v27+-blue?style=flat-square&logo=docker&logoColor)<br>
Chuẩn hóa hạ tầng Container

- **Nhất quán môi trường**: Đóng gói toàn bộ các công nghệ nặng (Java, Python, Kafka, Postgres, Grafana) vào các Container độc lập. Đảm bảo "chạy được trên máy của bạn là chạy được trên mọi máy khác".

- **Triển khai thần tốc**: Khởi động và kết nối toàn bộ hệ thống mạng nội bộ giữa các dịch vụ chỉ bằng một câu lệnh duy nhất: `docker-compose up -d`.

## **🛠 1. Quy trình xây dựng (Step-by-Step)**

### **Bước 1**: Khởi tạo dự án và Môi trường ảo (venv)

Mở terminal tại thư mục gốc của dự án:

```powershell
python -m venv venv
# Kích hoạt venv
# Windows:
.\venv\Scripts\activate
code . # Để mở giao diện vscode
# Linux/Mac:
source venv/bin/activate
code .
# Conda:
conda create -n weather_env python=3.11 -y
conda activate weather_env
code .
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
1. Tạo thư mục: `mkdir consumer -> cd consumer`.

2. Viết Script ETL: Tạo file `consumer_weather.py` sử dụng `foreachBatch` và `checkpointLocation`.

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

<img width="1841" height="948" alt="image" src="https://github.com/user-attachments/assets/50e725c7-8538-486e-bfd0-423d6458e7e1" />

## **💡 Lưu ý quan trọng**
**Checkpoint**: Nếu bạn sửa Schema của dữ liệu, hãy xóa thư mục `spark/checkpoints` trước khi khởi động lại để tránh lỗi xung đột metadata.

**Volume**: Thư mục `spark` được mount vào container để đảm bảo code và checkpoint không bị mất khi container bị tắt.
