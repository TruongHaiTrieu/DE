import os
from dotenv import load_dotenv

load_dotenv(override=True)

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, FloatType

KAFKA_SERVER = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
TOPIC = os.getenv("KAFKA_TOPIC")
USER = os.getenv("POSTGRES_USER")
PASSWORD = os.getenv("POSTGRES_PASSWORD")
DB = os.getenv("POSTGRES_DB")

if not KAFKA_SERVER or not TOPIC:
    print(KAFKA_SERVER , TOPIC)
    raise ValueError("❌ Thiếu cấu hình KAFKA_BOOTSTRAP_SERVERS hoặc KAFKA_TOPIC trong .env")

# 1. Khởi tạo Spark Session 
spark = SparkSession.builder \
    .appName("Weather-Data-Pipeline") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0,com.clickhouse:clickhouse-jdbc:0.6.0") \
    .config("spark.driver.extraJavaOptions", "-Divy.default.ivy.user.dir=/tmp/ivy") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# 2. Schema khớp chính xác với Producer mới
schema = StructType([
    StructField("city", StringType()),
    StructField("temp", FloatType()),
    StructField("humidity", FloatType()),
    StructField("pressure", FloatType()),
    StructField("wind_speed", FloatType()),
    StructField("weather_desc", StringType()),
    StructField("timestamp", StringType())
])

# 3. Đọc từ Kafka (Dùng DNS nội bộ của Docker)
raw_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_SERVER) \
    .option("subscribe", TOPIC) \
    .option("startingOffsets", "earliest") \
    .load()

# 4. Xử lý dữ liệu
parsed_df = raw_df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*") \
    .withColumn("timestamp", to_timestamp(col("timestamp"), "yyyy-MM-dd HH:mm:ss"))

# 5. Hàm ghi vào Postgres
def write_to_postgres(df, epoch_id):
    if df.count() > 0:
        try:
            print(f"📦 Batch {epoch_id}: Đang ghi dữ liệu vào PostgreSQL...")
            
            jdbc_url = "jdbc:postgresql://postgres:5432/weather_db"
            properties = {
                "user": USER,
                "password": PASSWORD,
                "driver": "org.postgresql.Driver"
            }

            df.write.jdbc(url=jdbc_url, table=DB, mode="append", properties=properties)
            
            print(f"✅ Batch {epoch_id}: Ghi thành công!")
        except Exception as e:
            print(f"❌ Lỗi ghi PostgreSQL: {e}")



# 6. Chạy Stream
checkpoint_path = "/opt/spark/work-dir/checkpoints/weather_stream"

query = parsed_df.writeStream \
    .foreachBatch(write_to_postgres) \
    .option("checkpointLocation", checkpoint_path) \
    .outputMode("append") \
    .start()

print("🚀 Spark đang chờ dữ liệu...")
query.awaitTermination()