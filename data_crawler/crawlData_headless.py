from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from batdongsan import *
import random
import pandas as pd
from hdfs import InsecureClient
import os

# Khởi tạo danh sách lưu trữ dữ liệu và các biến cần thiết
index = 1
checkpoint_file = "./data/checkpoint.txt"
hdfs_url = "http://localhost:50070"  # Thay đổi URL của HDFS nếu cần
hdfs_path = "/user/hadoop/houses_v3.csv"

# Đọc checkpoint nếu tồn tại
if os.path.exists(checkpoint_file):
    with open(checkpoint_file, "r") as f:
        start_page = int(f.read().strip())
else:
    start_page = 1

client = InsecureClient(hdfs_url)


# Hàm để lưu checkpoint
def save_checkpoint(page):
    with open(checkpoint_file, "w") as f:
        f.write(str(page))


# Hàm để thu thập dữ liệu từ trang web
def crawl_data(start_page, end_page):
    global index
    for i in range(start_page, end_page):
        options = webdriver.ChromeOptions()
        user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.1 Safari/605.1.15",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_1) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.1 Safari/605.1.15",
        ]
        user_agent = random.choice(user_agents)
        options.add_argument(f"user-agent={user_agent}")
        options.add_argument("--headless")
        service = Service(executable_path="./chorme-drive/chromedriver_125.exe")
        driver = webdriver.Chrome(service=service, options=options)

        navigateToWeb("https://batdongsan.com.vn/nha-dat-ban/p" + str(i + 1), driver)

        df = getCsvHouses(driver, index)
        driver.quit()

        # Lưu trực tiếp vào HDFS
        with client.write(hdfs_path, encoding="utf-8", append=True) as writer:
            df.to_csv(writer, header=False, index=False)

        index += len(df)

        # Lưu checkpoint
        save_checkpoint(i + 1)


# Thu thập dữ liệu từ trang 1 đến trang 200
crawl_data(start_page, 2)

print(f"Dữ liệu đã được lưu vào HDFS tại {hdfs_path}")
