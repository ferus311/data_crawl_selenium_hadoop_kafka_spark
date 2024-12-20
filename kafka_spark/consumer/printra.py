from pyspark.sql import SparkSession

# HDFS configuration
HDFS_INPUT_PATH = "hdfs://localhost:9000/real_estate_data"

# Initialize Spark session
spark = SparkSession.builder \
    .appName("ReadFromHDFS") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Read data from HDFS
df = spark.read.parquet(HDFS_INPUT_PATH)

# Get the last 10 records
last_records = df.tail(10)

# Show the last 10 records
for record in last_records:
    print(record)

# Print schema
df.printSchema()

# Stop the Spark session
spark.stop()
