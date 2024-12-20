from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.options import Options

from kafka import KafkaProducer
import json
import random
import time
from batdongsan import navigateToWeb, getCsvHouses

# Kafka configuration
KAFKA_TOPIC = "real_estate_data"
KAFKA_SERVER = "localhost:9092"

# Khởi tạo Kafka producer
producer = KafkaProducer(
    bootstrap_servers=[KAFKA_SERVER],
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

# Khởi tạo danh sách user agents
user_agents = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, như Gecko) Version/16.1 Safari/605.1.15",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_1) AppleWebKit/605.1.15 (KHTML, như Gecko) Version/16.1 Safari/605.1.15",
]


# Thiết lập WebDriver
def init_driver():
    options = webdriver.ChromeOptions()
    user_agent = random.choice(user_agents)
    options.add_argument(f"user-agent={user_agent}")
    options.add_argument("--headless")
    service = Service(ChromeDriverManager().install())
    return webdriver.Chrome(service=service, options=options)


# Hàm thu thập dữ liệu và gửi vào Kafka
def crawl_data(start_page, end_page):
    driver = init_driver()
    for i in range(start_page, end_page):
        try:
            url = f"https://batdongsan.com.vn/nha-dat-ban/p{i + 1}"
            navigateToWeb(url, driver)
            df = getCsvHouses(driver)

            # Gửi từng bản ghi vào Kafka
            for record in df.to_dict(orient="records"):
                producer.send(KAFKA_TOPIC, record)
                print(f"Sent to Kafka: {record}")

            time.sleep(2)  # Tránh quá tải server
        except Exception as e:
            print(f"Error on page {i + 1}: {e}")
        finally:
            producer.flush()
    driver.quit()


# Thu thập dữ liệu từ trang 1 đến trang 5
crawl_data(0, 5)
