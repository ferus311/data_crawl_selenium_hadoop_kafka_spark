from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType
from pyspark.sql.functions import from_json, col

# Kafka configuration
KAFKA_BROKER = "localhost:9092"
KAFKA_TOPIC = "real_estate_data"
HDFS_OUTPUT_PATH = "hdfs://localhost:9000/real_estate_data"
CHECKPOINT_LOCATION = "hdfs://localhost:9000/checkpoints/real_estate_data"

# Initialize Spark session
spark = SparkSession.builder \
    .appName("KafkaToHadoop") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Define schema for JSON data
schema = StructType() \
    .add("price", StringType()) \
    .add("area", StringType()) \
    .add("price_per_m2", StringType()) \
    .add("location", StringType()) \
    .add("bedroom", StringType()) \
    .add("bathroom", StringType())

# Read data from Kafka
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("subscribe", KAFKA_TOPIC) \
    .option("startingOffsets", "earliest") \
    .option("failOnDataLoss", "false") \
    .load()

# Convert value column to string and parse JSON
json_df = df.selectExpr("CAST(value AS STRING) as json_value")
json_df = json_df.select(from_json(col("json_value"), schema).alias("data")).select("data.*")

# Write data to HDFS
query = json_df \
    .writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", HDFS_OUTPUT_PATH) \
    .option("checkpointLocation", CHECKPOINT_LOCATION) \
    .start()

# Wait for the termination of the query
query.awaitTermination()
