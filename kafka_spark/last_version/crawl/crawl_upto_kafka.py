from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from kafka import KafkaProducer
import pandas as pd
from batdongsan import navigateToWeb, getCsvHouses
import json
import random
import os
import time

# Kafka configuration
KAFKA_TOPIC = "real_estate_data"
KAFKA_SERVER = "localhost:9092"

# Initialize Kafka producer with Exactly Once Semantics
producer = KafkaProducer(
    bootstrap_servers=[KAFKA_SERVER],
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    retries=5,
    acks='all'  # Ensure all replicas acknowledge the message
)

# User agent list
user_agents = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, như Gecko) Version/16.1 Safari/605.1.15",
]

# Checkpoint file
CHECKPOINT_FILE = "checkpoint.txt"
ERROR_LOG_FILE = "error.log"

# Function to initialize Chrome WebDriver
def init_driver():
    options = webdriver.ChromeOptions()
    user_agent = random.choice(user_agents)
    options.add_argument(f"user-agent={user_agent}")
    options.add_argument("--headless")
    service = Service(ChromeDriverManager().install())
    return webdriver.Chrome(service=service, options=options)

def load_checkpoint():
    if os.path.exists(CHECKPOINT_FILE):
        with open(CHECKPOINT_FILE, "r") as f:
            return int(f.read().strip())
    return 0

def save_checkpoint(page):
    with open(CHECKPOINT_FILE, "w") as f:
        f.write(str(page))

def log_error(message):
    with open(ERROR_LOG_FILE, "a") as f:
        f.write(f"{time.strftime('%Y-%m-%d %H:%M:%S')} - {message}\n")

# Crawl data
def crawl_data(start_page, end_page):
    driver = init_driver()
    last_page = load_checkpoint()  # Load checkpoint
    print(f"Resuming from page: {last_page + 1}")

    for i in range(max(start_page, last_page), end_page):
        retries = 3  # Retry crawling a page 3 times before giving up
        while retries > 0:
            try:
                url = f"https://batdongsan.com.vn/nha-dat-ban/p{i + 1}"
                print(f"Crawling page: {i + 1}")
                navigateToWeb(url, driver)
                df = getCsvHouses(driver)
                print(f"Retrieved {len(df)} records from page {i + 1}.")

                # Send data to Kafka
                for record in df.to_dict(orient="records"):
                    producer.send(KAFKA_TOPIC, record)
                    print(f"Sent to Kafka: {record}")

                producer.flush()  # Ensure all messages are sent
                save_checkpoint(i + 1)  # Save progress
                time.sleep(2)  # Avoid server overload
                break  # Exit retry loop if successful

            except Exception as e:
                log_error(f"Error on page {i + 1}: {e}")
                print(f"Error on page {i + 1}: {e}")
                retries -= 1
                time.sleep(5)  # Wait before retrying
        else:
            print(f"Skipping page {i + 1} after multiple failures.")
    driver.quit()

# Run the crawler
if __name__ == "__main__":
    START_PAGE = 0
    END_PAGE = 200
    crawl_data(START_PAGE, END_PAGE)
    print("Crawling completed.")
