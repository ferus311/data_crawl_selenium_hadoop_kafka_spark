from pyspark.sql import SparkSession

# HDFS configuration
HDFS_INPUT_PATH = "hdfs://localhost:9000/gr2/processed_data"

# Initialize Spark session
spark = SparkSession.builder \
    .appName("ViewAllDataFromHDFS") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Read data from HDFS

df = spark.read.parquet(HDFS_INPUT_PATH)

# Show all data excluding the 'description' and 'title' columns
columns_to_show = [col for col in df.columns if col not in ['description']]
df.select(columns_to_show).show(truncate=False)

# Print schema
df.printSchema()

# Count the number of rows
row_count = df.count()
print(f"Number of rows: {row_count}")

# Stop the Spark session
spark.stop()
