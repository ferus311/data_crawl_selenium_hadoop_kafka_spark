from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, udf, when, split, lit
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    FloatType,
    IntegerType,
)

# HDFS paths
HDFS_INPUT_PATH = "hdfs://localhost:9000/gr2/data_v2"
HDFS_OUTPUT_PATH = "hdfs://localhost:9000/gr2/processed_data"

# Create Spark session
spark = (
    SparkSession.builder.appName("ProcessRealEstateData")
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0")
    .config("spark.hadoop.fs.defaultFS", "hdfs://localhost:9000")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# Schema definition
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

# Read data from HDFS
df = spark.read.parquet(HDFS_INPUT_PATH)

# UDF to safely convert to integer
def safe_to_int(value):
    try:
        return int(value)
    except (ValueError, TypeError):
        return None

safe_to_int_udf = udf(safe_to_int, IntegerType())

# Function to clean and convert price to float
def clean_price(value):
    if value is None:
        return None
    value = value.replace(",", ".").lower()

    # Handle 'tỷ', 'triệu', and 'tr/m²' and convert to numeric
    if "tỷ" in value:
        try:
            return float(value.replace(" tỷ", "").strip()) * 1e9
        except ValueError:
            return None
    elif "triệu" in value:
        try:
            return float(value.replace(" triệu", "").strip()) * 1e6
        except ValueError:
            return None
    elif "tr/m²" in value:
        try:
            return float(value.replace(" tr/m²", "").strip()) * 1e6
        except ValueError:
            return None
    # If it's a numeric string
    elif value.replace(".", "").isdigit():
        try:
            return float(value)
        except ValueError:
            return None
    return None

clean_price_udf = udf(clean_price, FloatType())

# Function to clean and convert area to float
def clean_area(value):
    if value is None:
        return None
    value = value.replace(" m²", "").strip()
    try:
        return float(value)
    except ValueError:
        return None

clean_area_udf = udf(clean_area, FloatType())

# Apply cleaning and conversion
df = df.withColumn("bedroom", safe_to_int_udf(col("bedroom")))
df = df.withColumn("bathroom", safe_to_int_udf(col("bathroom")))
df = df.withColumn("area", clean_area_udf(col("area")))
df = df.withColumn("price_per_m2", clean_price_udf(col("price_per_m2")))

# Remove rows where price_per_m2 or area is null
df = df.filter(col("price_per_m2").isNotNull() & col("area").isNotNull())

# Set default value of 1 for null or missing bedroom and bathroom
df = df.withColumn(
    "bedroom", when(col("bedroom").isNull(), 1).otherwise(col("bedroom"))
)
df = df.withColumn(
    "bathroom", when(col("bathroom").isNull(), 1).otherwise(col("bathroom"))
)

# Handle "Giá thỏa thuận"
df = df.withColumn(
    "price",
    when(
        col("price").like("%Giá thỏa thuận%"),
        when(col("price_per_m2").isNotNull() & col("area").isNotNull(), col("price_per_m2") * col("area")).otherwise(lit(None))
    ).otherwise(clean_price_udf(col("price"))),
)

# Split location into district and city, handle cases where the location might not have two parts
df = df.withColumn("district", split(col("location"), ",")[0])
df = df.withColumn("city", when(split(col("location"), ",").getItem(1).isNotNull(), split(col("location"), ",")[1]).otherwise(lit(None)))

# Drop unnecessary columns
# df = df.drop("description", "title", "location")

# Write processed data back to HDFS
df.write.mode("overwrite").parquet(HDFS_OUTPUT_PATH)

# Stop Spark session
spark.stop()
