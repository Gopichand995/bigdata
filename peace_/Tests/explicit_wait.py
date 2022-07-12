from selenium import webdriver
from selenium.webdriver.common.by import By
import time

driver = webdriver.Chrome("C:\\Users\\GopichandBarri\\Documents\\Github\\peace\\chromedriver.exe")

driver.get("https://www.expedia.com/")
driver.maximize_window()

# driver.find_element(By.ID, "")