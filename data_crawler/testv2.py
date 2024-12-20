from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager

import random
import pandas as pd
from batdongsan import *
import os
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
import time

# Khởi tạo danh sách lưu trữ dữ liệu và các biến cần thiết
checkpoint_file = "./data/checkpoint.txt"

if os.path.exists(checkpoint_file):
    with open(checkpoint_file, "r") as f:
        start_page = int(f.read().strip())
else:
    start_page = 0


def save_checkpoint(page):
    with open(checkpoint_file, "w") as f:
        f.write(str(page))


# Thiết lập webdriver và các tùy chọn
options = webdriver.ChromeOptions()
user_agents = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, như Gecko) Version/16.1 Safari/605.1.15",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_1) AppleWebKit/605.1.15 (KHTML, như Gecko) Version/16.1 Safari/605.1.15",
]
user_agent = random.choice(user_agents)
options.add_argument(f"user-agent={user_agent}")
options.add_argument("--headless")
service = Service(ChromeDriverManager().install())
driver = webdriver.Chrome(service=service, options=options)

# Khởi tạo Spark session
spark = (
    SparkSession.builder.appName("CrawlData")
    .config("spark.hadoop.fs.defaultFS", "hdfs://localhost:9000")
    .getOrCreate()
)


# Hàm để thu thập dữ liệu từ trang web
def crawl_data(start_page, end_page):
    for i in range(start_page, end_page):
        try:
            # Navigate to the webpage (replace navigateToWeb with driver.get)
            navigateToWeb(
                "https://batdongsan.com.vn/nha-dat-ban/p" + str(i + 1), driver
            )
            df = getCsvHouses(driver)
            # Convert the DataFrame to a Spark DataFrame
            spark_df = spark.createDataFrame(df)

            # Drop duplicates from the DataFrame
            spark_df = spark_df.dropDuplicates()

            # Write the data to HDFS in Parquet format
            spark_df.write.mode("append").parquet("hdfs:///data/")

            # Save the current page number to checkpoint
            save_checkpoint(i + 1)

            # Optional: Sleep to avoid hitting the server too fast
            time.sleep(2)

        except Exception as e:
            print(f"Error on page {i + 1}: {e}")


# Thu thập dữ liệu từ trang 1 đến trang 20
crawl_data(start_page, 5)


crawled_data = spark.read.parquet("hdfs:///data/")
crawled_data.show(10)

# Đóng driver khi hoàn thành
driver.quit()

# Dừng Spark session
spark.stop()
