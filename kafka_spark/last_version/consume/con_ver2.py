from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, udf
from pyspark.sql.types import StructType, StructField, StringType, FloatType

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

# UDFs để xử lý giá trị chuỗi (loại bỏ ký tự không cần thiết, chuyển thành giá trị số)
def convert_price_to_number(price_str):
    try:
        price_str = price_str.replace(",", "")  # Xóa dấu phẩy
        if 'tỷ' in price_str:
            return float(price_str.replace('tỷ', '')) * 1e9
        elif 'tr' in price_str:
            return float(price_str.replace('tr', '')) * 1e6
        return float(price_str)
    except:
        return None

def convert_area_to_number(area_str):
    try:
        return float(area_str.replace(" m²", "").replace(",", ""))
    except:
        return None

def convert_price_per_m2(price_per_m2_str):
    try:
        return float(price_per_m2_str.replace(" tr/m²", "").replace(",", ""))
    except:
        return None

# Đăng ký UDFs
convert_price_to_number_udf = udf(convert_price_to_number, FloatType())
convert_area_to_number_udf = udf(convert_area_to_number, FloatType())
convert_price_per_m2_udf = udf(convert_price_per_m2, FloatType())

# Áp dụng UDFs để chuyển đổi các cột
processed_df = json_df \
    .withColumn("price", convert_price_to_number_udf(col("price"))) \
    .withColumn("area", convert_area_to_number_udf(col("area"))) \
    .withColumn("price_per_m2", convert_price_per_m2_udf(col("price_per_m2")))

# Lưu dữ liệu lên HDFS
query = processed_df \
    .writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", "hdfs://localhost:9000/real_estate_data") \
    .option("checkpointLocation", "hdfs://localhost:9000/checkpoints/real_estate_data") \
    .start()

# Chờ đến khi stream kết thúc
query.awaitTermination()
