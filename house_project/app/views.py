from django.shortcuts import render
from django.http import JsonResponse
from pyspark.sql import SparkSession
from django.views.decorators.http import require_GET
from urllib.parse import unquote, unquote_plus
import logging

# HDFS configuration
HDFS_INPUT_PATH = "hdfs://localhost:9000/gr2/processed_data"

# Initialize Spark session
spark = SparkSession.builder.appName("ViewAllDataFromHDFS").getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Set up logging
logger = logging.getLogger(__name__)


def index(request):
    # Read data from HDFS
    df = spark.read.parquet(HDFS_INPUT_PATH)

    # Convert DataFrame to dictionary
    data = df.toPandas().to_dict(orient="records")

    return render(request, "index.html", {"data": data})


def get_data(request):
    # Read data from HDFS
    df = spark.read.parquet(HDFS_INPUT_PATH)

    # Convert DataFrame to JSON
    data = df.toJSON().collect()

    return JsonResponse(data, safe=False)


def get_schema(request):
    # Read data from HDFS
    df = spark.read.parquet(HDFS_INPUT_PATH)

    # Get schema
    schema = df.schema.json()

    return JsonResponse(schema, safe=False)


def get_count(request):
    # Read data from HDFS
    df = spark.read.parquet(HDFS_INPUT_PATH)

    # Count the number of rows
    row_count = df.count()

    return JsonResponse({"row_count": row_count})


@require_GET
def search(request):
    # Read data from HDFS
    df = spark.read.parquet(HDFS_INPUT_PATH)

    # Get query parameters
    price = request.GET.get("price")
    district = unquote(request.GET.get("district", ""))
    city = unquote(request.GET.get("city", ""))

    # Apply filters
    if price:
        df = df.filter(df.price <= float(price))
    if district:
        df = df.filter(df.district == district)
    if city:
        df = df.filter(df.city == city)

    # Convert DataFrame to JSON
    data = df.toJSON().collect()

    return JsonResponse({"houses": data}, safe=False)


@require_GET
def get_cities(request):
    # Read data from HDFS
    df = spark.read.parquet(HDFS_INPUT_PATH)

    # Get unique cities
    cities = df.select("city").distinct().rdd.map(lambda row: row.city).collect()

    return JsonResponse(cities, safe=False)


@require_GET
def get_districts(request):
    # Read data from HDFS
    df = spark.read.parquet(HDFS_INPUT_PATH)

    # Get query parameter
    city = unquote(request.GET.get("city", ""))

    # Get unique districts based on city
    if city:
        districts = (
            df.filter(df.city == city)
            .select("district")
            .distinct()
            .rdd.map(lambda row: row.district)
            .collect()
        )
    else:
        districts = (
            df.select("district").distinct().rdd.map(lambda row: row.district).collect()
        )

    return JsonResponse(districts, safe=False)


@require_GET
def get_price_distribution(request):
    # Read data from HDFS
    df = spark.read.parquet(HDFS_INPUT_PATH)

    # Get price distribution
    price_distribution = df.groupBy("price").count().orderBy("price").collect()

    data = [
        {"price": row["price"], "count": row["count"]} for row in price_distribution
    ]

    return JsonResponse(data, safe=False)


@require_GET
def get_area_distribution(request):
    # Read data from HDFS
    df = spark.read.parquet(HDFS_INPUT_PATH)

    # Get area distribution
    area_distribution = df.groupBy("area").count().orderBy("area").collect()

    data = [{"area": row["area"], "count": row["count"]} for row in area_distribution]

    return JsonResponse(data, safe=False)


def charts(request):
    return render(request, "charts.html")
