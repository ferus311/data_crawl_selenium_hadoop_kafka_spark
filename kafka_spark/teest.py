from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType

# Tạo Spark session
spark = SparkSession.builder \
    .appName("KafkaSparkStreaming") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://localhost:9000") \
    .getOrCreate()

# Đọc dữ liệu từ Kafka topic
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "real_estate_data") \
    .option("startingOffsets", "earliest") \
    .load()

# Chuyển đổi dữ liệu từ Kafka (Kafka lưu dưới dạng key và value, bạn cần lấy value)
# Giả sử dữ liệu là JSON
json_df = df.selectExpr("CAST(value AS STRING) as json_value")

# Định nghĩa schema cho dữ liệu JSON
schema = StructType([
    StructField("price", StringType(), True),
    StructField("area", StringType(), True),
    StructField("price_per_m2", StringType(), True),
    StructField("location", StringType(), True),
    StructField("bedroom", StringType(), True),
    StructField("bathroom", StringType(), True)
])

# Chuyển đổi value từ JSON thành các cột cụ thể
json_df = json_df.select(from_json(col("json_value"), schema).alias("data")).select("data.*")

# Tạo một streaming query để hiển thị dữ liệu
query = json_df \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .option("path", "hdfs://localhost:9000/real_estate_data") \
    .option("truncate", "false") \
    .start()

# Chờ đến khi stream kết thúc
query.awaitTermination()
