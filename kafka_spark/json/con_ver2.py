from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, udf
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.types import IntegerType

HDFS = "hdfs://localhost:9000/gr2/data_v2"
KAFKA_SERVER = "localhost:9092"
CHECKPOINT_LOCATION = "hdfs://localhost:9000/gr2/checkpoint"

# Tạo Spark session
spark = (
    SparkSession.builder.appName("KafkaSparkStreaming")
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0")
    .config("spark.hadoop.fs.defaultFS", "hdfs://localhost:9000")
    .getOrCreate()
)

# Đọc dữ liệu từ Kafka topic
df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_SERVER)
    .option("subscribe", "real_estate_data")
    .option("startingOffsets", "earliest")
    .load()
)

# Chuyển đổi dữ liệu từ Kafka (Kafka lưu dưới dạng key và value, bạn cần lấy value)
# Giả sử dữ liệu là JSON
json_df = df.selectExpr("CAST(value AS STRING) as json_value")

# Định nghĩa schema cho dữ liệu JSON
schema = StructType(
    [
        StructField("title", StringType(), True),
        StructField("price", StringType(), True),
        StructField("area", StringType(), True),
        StructField("price_per_m2", StringType(), True),
        StructField("location", StringType(), True),
        StructField("bedroom", StringType(), True),  # Để nguyên là StringType
        StructField("bathroom", StringType(), True),  # Để nguyên là StringType
        StructField("description", StringType(), True),
    ]
)

# Chuyển đổi value từ JSON thành các cột cụ thể
json_df = json_df.select(from_json(col("json_value"), schema).alias("data")).select("data.*")

# Định nghĩa UDF để chuyển đổi chuỗi thành số nguyên
def safe_to_int(value):
    try:
        return int(value)
    except (ValueError, TypeError):
        return None

safe_to_int_udf = udf(safe_to_int, IntegerType())

# Áp dụng UDF để chuyển đổi các cột "bedroom" và "bathroom" thành số nguyên
json_df = json_df.withColumn("bedroom", safe_to_int_udf(col("bedroom")))
json_df = json_df.withColumn("bathroom", safe_to_int_udf(col("bathroom")))

# Ghi dữ liệu vào HDFS
query = json_df.writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", HDFS) \
    .option("checkpointLocation", CHECKPOINT_LOCATION) \
    .start()

# Chờ đến khi stream kết thúc
query.awaitTermination()
