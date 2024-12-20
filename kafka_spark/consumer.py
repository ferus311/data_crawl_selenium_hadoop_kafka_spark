from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType

# Kafka configuration
KAFKA_TOPIC = "real_estate_data"
KAFKA_SERVER = "localhost:9092"

# Khởi tạo Spark session
spark = (
    SparkSession.builder.appName("RealEstateStreaming")
    .config("spark.sql.streaming.checkpointLocation", "/tmp/spark-checkpoints")
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0")  # Cấu hình package Kafka
    .getOrCreate()
)

# Schema cho dữ liệu (đảm bảo kiểu dữ liệu hợp lý)
schema = StructType() \
    .add("price", StringType()) \
    .add("area", StringType()) \
    .add("price_per_m2", StringType()) \
    .add("location", StringType()) \
    .add("bedroom", StringType()) \
    .add("bathroom", StringType())

# Đọc dữ liệu từ Kafka
df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_SERVER)
    .option("subscribe", KAFKA_TOPIC)
    .load()
)

# Giải mã giá trị từ Kafka (chuyển đổi từ Kafka message value thành chuỗi)
json_df = df.selectExpr("CAST(value AS STRING) as json_data")

# Phân tích dữ liệu JSON và chuyển đổi thành dataframe với schema đã định nghĩa
parsed_df = (
    json_df.selectExpr("json_data")
    .selectExpr("from_json(json_data, schema) as data")
    .select("data.*")
)

# Lưu dữ liệu vào HDFS dưới định dạng Parquet
query = (
    parsed_df.writeStream.format("parquet")
    .outputMode("append")
    .option("checkpointLocation", "/tmp/spark-checkpoints")  # Đảm bảo lưu checkpoint
    .option("path", "hdfs:///data/real_estate/")  # Đường dẫn HDFS cho output
    .start()
)

# Chờ đợi cho đến khi stream kết thúc
query.awaitTermination()
