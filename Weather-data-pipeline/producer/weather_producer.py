import os
import time
import json
import requests
import datetime
from kafka import KafkaProducer
from dotenv import load_dotenv

# 1. Load cấu hình từ file .env
load_dotenv()

API_KEY = os.getenv("OPENWEATHER_API_KEY")
CITY = os.getenv("CITY_NAME")
KAFKA_SERVER = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
TOPIC = os.getenv("KAFKA_TOPIC")

# 2. Khởi tạo Kafka Producer
# Thay vì truyền trực tiếp biến string KAFKA_SERVER
# Hãy biến nó thành một danh sách (list) bằng cách dùng .split(',')
try:
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_SERVER.split(','), # Thêm .split(',') ở đây
        #bootstrap_servers=[KAFKA_SERVER],
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        acks='all',
        retries=5
    )
    print(f"✅ Đã kết nối thành công tới Kafka tại {KAFKA_SERVER}")
except Exception as e:
    print(f"❌ Không thể kết nối tới Kafka: {e}")
    exit(1)

# def transform_weather_data(raw):
#     """Trích xuất từ JSON OpenWeather để khớp với Schema ClickHouse"""
#     try:
#         # Sử dụng .get() để tránh lỗi crash nếu API thiếu trường dữ liệu
#         main = raw.get("main", {})
#         weather = raw.get("weather", [{}])[0]
#         wind = raw.get("wind", {})

#         # Nếu 'main' trống (do lỗi API), dừng xử lý
#         if not main:
#             print(f"⚠️ Dữ liệu không hợp lệ từ API: {raw}")
#             return None

#         return {
#             "city": raw.get("name"),
#             "temperature": float(main.get("temp")),
#             "humidity": float(main.get("humidity")),
#             "wind_speed": float(wind.get("speed")),
#             "description": weather.get("description"),
#             # dt là Unix timestamp, cần chuyển sang định dạng SQL của ClickHouse
#             "timestamp": datetime.datetime.fromtimestamp(raw.get("dt")).strftime('%Y-%m-%d %H:%M:%S')
#         }
#     except Exception as e:
#         print(f"⚠️ Lỗi xử lý dữ liệu (Transform Error): {e}")
#         return None

def transform_weather_data(raw):
    """Trích xuất các đặc trưng cần thiết từ JSON thô của OpenWeather"""
    try:
        return {
            "city": raw.get("name"),
            "temp": raw.get("main", {}).get("temp"),
            "humidity": raw.get("main", {}).get("humidity"),
            "pressure": raw.get("main", {}).get("pressure"),
            "wind_speed": raw.get("wind", {}).get("speed"),
            "weather_desc": raw.get("weather", [{}])[0].get("description"),
            "timestamp": datetime.datetime.fromtimestamp(raw.get("dt")).strftime('%Y-%m-%d %H:%M:%S')
        }
    except Exception as e:
        print(f"Lỗi xử lý dữ liệu: {e}")
        return None

def fetch_and_send():
    """Gửi dữ liệu sạch vào Kafka"""
    url = f"http://api.openweathermap.org/data/2.5/weather?q={CITY}&appid={API_KEY}&units=metric"
    
    print(f"🚀 Bắt đầu lấy dữ liệu cho {CITY}...")
    
    while True:
        try:
            response = requests.get(url, timeout=10)
            if response.status_code == 200:
                raw_data = response.json()
                
                # Trích xuất đặc trưng (Giai đoạn chuyển đổi thô sang sạch)
                clean_data = transform_weather_data(raw_data)
                
                if clean_data:
                    # Gửi vào Kafka
                    producer.send(TOPIC, value=clean_data)
                    # flush() để đảm bảo dữ liệu được gửi ngay lập tức
                    producer.flush() 
                    print(f"📤 Sent: {clean_data['timestamp']} | {clean_data['temp']}°C | {clean_data['weather_desc']}")
            else:
                print(f"❌ Lỗi API: {response.status_code} - {response.text}")
                
        except Exception as e:
            print(f"❌ Lỗi hệ thống: {e}")
        
        # Nghỉ 30 giây để tránh bị khóa API (Rate Limit)
        time.sleep(30)

if __name__ == "__main__":
    try:
        fetch_and_send()
    except KeyboardInterrupt:
        print("\n🛑 Đã dừng Producer.")
    finally:
        producer.close()