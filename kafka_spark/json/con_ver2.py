from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, udf, when
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

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
    .option("failOnDataLoss", "false")  # Thêm tùy chọn này để tránh lỗi khi mất dữ liệu
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
        StructField("bedroom", StringType(), True),
        StructField("bathroom", StringType(), True),
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

# Định nghĩa UDF để chuyển đổi chuỗi thành số thực và quy đổi đơn vị
def convert_price(value):
    try:
        value = value.replace(",", "").replace(" m²", "").replace(" tr/m²", "").replace(" tỷ", "000000000").replace(" triệu", "000000")
        return float(value)
    except (ValueError, TypeError):
        return None

convert_price_udf = udf(convert_price, FloatType())

# Áp dụng UDF để chuyển đổi các cột "bedroom", "bathroom", "area", và "price_per_m2" thành số
json_df = json_df.withColumn("bedroom", safe_to_int_udf(col("bedroom")))
json_df = json_df.withColumn("bathroom", safe_to_int_udf(col("bathroom")))
json_df = json_df.withColumn("area", convert_price_udf(col("area")))
json_df = json_df.withColumn("price_per_m2", convert_price_udf(col("price_per_m2")))

# Tính toán giá cho các ngôi nhà có trường "price" là "Giá thỏa thuận"
json_df = json_df.withColumn("price", when(col("price") == "Giá thỏa thuận", col("price_per_m2") * col("area")).otherwise(col("price")))

# Chuyển đổi cột "price" thành số thực và quy đổi đơn vị
json_df = json_df.withColumn("price", convert_price_udf(col("price")))

# Loại bỏ các trường không cần thiết
json_df = json_df.drop("title", "description")

# Ghi dữ liệu đã xử lý vào HDFS
query = json_df.writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", HDFS) \
    .option("checkpointLocation", CHECKPOINT_LOCATION) \
    .start()

# Chờ đến khi stream kết thúc
query.awaitTermination()
