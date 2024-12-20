from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace, when
import requests
from pyspark.sql.functions import udf
from pyspark.sql.types import StructType, StructField, FloatType

API_KEY = '18bc025aebb94471afa57bb58777fa41'
# ham chuyen doi location
def get_coordinates(address):
    url = f"https://api.opencagedata.com/geocode/v1/json?q={address}&key={API_KEY}&language=vi"
    response = requests.get(url).json()
    if response['results']:
        coordinates = response['results'][0]['geometry']
        return (coordinates['lat'], coordinates['lng'])
    else:
        return (None, None)

get_coordinates_udf = udf(get_coordinates, StructType([StructField("latitude", FloatType()), StructField("longitude", FloatType())]))


# Khởi tạo SparkSessionSS
spark = SparkSession.builder \
    .appName("CSV Processing Example") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://localhost:9000") \
    .getOrCreate()

# Đọc dữ liệu từ HDFS
df = spark.read.csv("hdfs:///gr1/houses_data.csv", header=True, inferSchema=True)

# Hiển thị dữ liệu
df.show()
df = df.filter(col("price_per_m2").isNotNull())
df = df.drop("id")
# Loại bỏ các đơn vị trong cột price, area và price_per_m2
df = df.withColumn("price", regexp_replace(col("price"), "[ tỷ]", ""))
df = df.withColumn("area", regexp_replace(col("area"), " m²", ""))
df = df.withColumn("price_per_m2", regexp_replace(col("price_per_m2"), " tr/m²", ""))

# Chuyển đổi dấu phẩy thành dấu chấm trong các cột price, area và price_per_m2
df = df.withColumn("price", regexp_replace(col("price"), ",", "."))
df = df.withColumn("area", regexp_replace(col("area"), ",", "."))
df = df.withColumn("price_per_m2", regexp_replace(col("price_per_m2"), ",", "."))

# Chuyển các cột về kiểu số
df = df.withColumn("price", col("price").cast("double"))
df = df.withColumn("area", col("area").cast("double"))
df = df.withColumn("price_per_m2", col("price_per_m2").cast("double"))

df = df.filter(col("price").isNotNull())
# Thay thế giá trị "Giá thỏa thuận" trong cột price bằng giá trị price_per_m2 * area
# df = df.withColumn("price", when(col("price").isNull(), col("price_per_m2") * col("area")).otherwise(col("price")))

# Thêm các trường thiếu và điền giá trị mặc định
df = df.withColumn("bedroom", col("bedroom").cast("int"))
df = df.withColumn("bathroom", col("bathroom").cast("int"))
df = df.na.fill({"bedroom": 1, "bathroom": 1})

# Thêm tọa độ vào DataFrame
df_with_coordinates = df.withColumn("coordinates", get_coordinates_udf(df["location"]))
df_with_coordinates = df_with_coordinates.withColumn("latitude", df_with_coordinates["coordinates"].getItem("latitude")) \
                                         .withColumn("longitude", df_with_coordinates["coordinates"].getItem("longitude")) \
                                         .drop("coordinates")

# Hiển thị dữ liệu đã xử lý
df.show()

# Ghi kết quả ra HDFS
df.write.mode("overwrite").option("header", "true").csv("hdfs:///processed_data//houses_data_v1.csv")

# Dừng SparkSession
spark.stop()








