from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("CheckParquetData").getOrCreate()

# Đọc file Parquet từ HDFS
df = spark.read.parquet("hdfs://localhost:9000/real_estate/output")
error_df = spark.read.parquet("hdfs://localhost:9000/real_estate/error_data")

# Hiển thị dữ liệu
print("Dữ liệu đã parse thành công:")
df.show(truncate=False)

print("Dữ liệu lỗi:")
error_df.show(truncate=False)
