from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager

from batdongsan import *
import random
import pandas as pd
import os

# Khởi tạo danh sách lưu trữ dữ liệu và các biến cần thiết
checkpoint_file = "./data/checkpoint.txt"
csv_file = "./data/house_v1.csv"

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

# Hàm để thu thập dữ liệu từ trang web
def crawl_data(start_page, end_page):
    for i in range(start_page, end_page):
        print(f"Đang crawl dữ liệu từ trang {i + 1}...")  # Debug: Hiển thị trang đang crawl
        navigateToWeb("https://batdongsan.com.vn/nha-dat-ban/p" + str(i + 1), driver)
        df = getCsvHouses(driver)
        print(f"Đã lấy được {len(df)} dòng dữ liệu từ trang {i + 1}.")  # Debug: Hiển thị số dòng dữ liệu lấy được

        # Lưu trực tiếp vào CSV
        if not os.path.exists(csv_file):
            df.to_csv(csv_file, mode='w', header=True, index=False, encoding='utf-8')
            print(f"Đã tạo tệp {csv_file} và lưu dữ liệu.")  # Debug: Hiển thị khi tạo tệp mới
        else:
            df.to_csv(csv_file, mode='a', header=False, index=False, encoding='utf-8')
            print(f"Đã thêm dữ liệu vào tệp {csv_file}.")  # Debug: Hiển thị khi thêm dữ liệu vào tệp hiện có

        # Lưu checkpoint
        save_checkpoint(i + 1)
        print(f"Đã lưu checkpoint tại trang {i + 1}.")  # Debug: Hiển thị khi lưu checkpoint

# Thu thập dữ liệu từ trang 1 đến trang 200
crawl_data(start_page, 200)

print(f"Dữ liệu đã được lưu vào {csv_file}")
