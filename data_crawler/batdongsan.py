from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
import pandas as pd
import json


class House:
    def __init__(self,id,title,price,images,area,price_per_m2,location,description):
        self.id = id
        self.price = price
        self.title = title
        self.images = images
        self.price_per_m2 = price_per_m2
        self.location = location
        self.description = description
        self.area = area

def navigateToWeb(string, driver):
    driver.maximize_window()
    driver.get(string)


def findImages(element):
    try:
        imageURLs = element.find_elements(By.XPATH, ".//div[@class='re__card-image ']//img")
        return [ imageURL.get_attribute('src') for imageURL in imageURLs]
    except:
        return None

def findTitle(element):
    try:
        title = element.find_element(By.XPATH, ".//h3[@class= 're__card-title']//span[@class='pr-title js__card-title']")
        return title.text
    except:
        return None

def findPrice(element):
    try:
        price = element.find_element(By.XPATH, ".//span[@class='re__card-config-price js__card-config-item']")
        return price.text
    except:
        return None

def findPricePerM2(element):
    try:
        PricePerM2 = element.find_element(By.XPATH, ".//span[@class='re__card-config-price_per_m2 js__card-config-item']")
        return PricePerM2.text
    except:
        return None


def findArea(element):
    try:
        area = element.find_element(By.XPATH, ".//span[@class='re__card-config-area js__card-config-item']")
        return area.text
    except:
        return None

def findLocation(element):
    try:
        area = element.find_element(By.XPATH, ".//div[@class='re__card-location']//span[2]")
        return area.text
    except:
        try:
            area = element.find_element(By.XPATH, ".//div[@class='re__card-location']//span")
            return area.text
        except:
            return None
        return None

def findDescription(element):
    try:
        description = element.find_element(By.XPATH, ".//div[@class='re__card-description js__card-description']")
        return description.text
    except:
        return None

def findBedroom(element):
    try:
        bedroom = element.find_element(By.XPATH, ".//span[@class = 're__card-config-bedroom js__card-config-item']/span")
        return bedroom.text
    except:
        return None

def findBathroom(element):
    try:
        bathroom = element.find_element(By.XPATH, ".//span[@class='re__card-config-toilet js__card-config-item']/span")
        return bathroom.text
    except:
        return None


def scrollDown(driver):
    scroll_speed = 50  # Tốc độ cuộn
    page_height = driver.execute_script("return document.body.scrollHeight")
    for i in range(0, page_height, scroll_speed):
        driver.execute_script(f"window.scrollTo(0, {i});")
        driver.implicitly_wait(0.1)

def scroolUp(driver):
    scroll_speed = 40  # Tốc độ cuộn
    page_height = driver.execute_script("return document.body.scrollHeight")
    for i in range(0, page_height, scroll_speed):
        driver.execute_script(f"window.scrollTo(0, {0-i});")
        driver.implicitly_wait(0.1)

def getArrayObjectHouses(driver):
    Houses = []
    elements = driver.find_elements(By.CLASS_NAME, "js__card")
    for element in elements:
        price = findPrice(element)
        title = findTitle(element)
        images = findImages(element)
        price_per_m2 = findPricePerM2(element)
        location = findLocation(element)
        description = findDescription(element)
        area = findArea(element)
        house = House(title,price,images, area,price_per_m2,location,description)
        Houses.append(house)
    return Houses

def getJsonHouses(driver, index):
    Houses = []
    elements = driver.find_elements(By.CLASS_NAME, "js__card")
    for element in elements:
        price = findPrice(element)
        title = findTitle(element)
        images = findImages(element)
        price_per_m2 = findPricePerM2(element)
        location = findLocation(element)
        description = findDescription(element)
        area = findArea(element)
        bedroom = findBedroom(element)
        bathroom = findBathroom(element)
        house_dict = {
            "id": index,
            "title": title,
            "price": price,
            "images": images,
            "area": area,
            "price_per_m2": price_per_m2,
            "location": location,
            "bedroom": bedroom,
            "bathroom": bathroom,
            "description": description
        }
        index += 1
        Houses.append(house_dict)
    return Houses

def getCsvHouses(driver):
    houses = []
    elements = driver.find_elements(By.CLASS_NAME, "js__card")
    for element in elements:
        price = findPrice(element)
        price_per_m2 = findPricePerM2(element)
        location = findLocation(element)
        area = findArea(element)
        bedroom = findBedroom(element)
        bathroom = findBathroom(element)
        house_dict = {
            "price": price,
            "area": area,
            "price_per_m2": price_per_m2,
            "location": location,
            "bedroom": bedroom,
            "bathroom": bathroom,
        }
        if location:
            houses.append(house_dict)
    return pd.DataFrame(houses)

def nextPage(driver):
    nextPage = driver.find_element(By.XPATH, "//div[@class='re__pagination-group']//a[@pid='2' and @class='re__pagination-number ']")
    scroll_position = nextPage.location['y'] - 250
    driver.execute_script("window.scrollTo(0, arguments[0]);", scroll_position)
    nextPage.click()

def writeToFile(file_path, element_list):
    with open(file_path, 'r') as file:
        try:
            data = json.load(file)
        except:
            with open(file_path, 'w') as file:
                json.dump(element_list, file, indent=4)
            return

    # Mảng chứa các đối tượng trong tệp JSON
    objects_array = element_list + data

    # Ghi mảng mới (bao gồm cả dữ liệu cũ và mới) vào tệp JSON
    with open(file_path, 'w') as file:
        json.dump(objects_array, file, indent=4)
