import time
import unittest
from selenium import webdriver
from selenium.webdriver.common.keys import Keys


driver = webdriver.Chrome("chromedriver.exe")

driver.get("https://www.facebook.com/")
print(driver.title)

time.sleep(5)

driver.get("https://www.amazon.in/")
print(driver.title)

driver.back()
print(driver.title)

time.sleep(5)

driver.forward()
print(driver.title)







# driver.get("http://demo.automationtesting.in/Windows.html")
# print(driver.title)
# print(driver.current_url)
# driver.find_element_by_xpath("//*[@id='Tabbed']/a/button").click()
# time.sleep(5)
# driver.quit()


# driver.get("https://www.facebook.com/")
# driver.find_element_by_id("email").send_keys("gopichand995@gmail.com")
# driver.find_element_by_id("pass").send_keys("password*")
# driver.find_element_by_name("login").click()


# driver.get("https://www.python.org")
# print(driver.title)
# search_bar = driver.find_element_by_name("q")
# search_bar.clear()
# search_bar.send_keys("getting started with python")
# search_bar.send_keys(Keys.RETURN)
# print(driver.current_url)
# driver.close()


# The code that switches focus to a new window is:
# driver.switch_to_window('window_name')


# To view a list of all window handles, run the following:
# print(driver.window_handles)


# class ChromeSearch(unittest.TestCase):
#     def setUp(self):
#         self.driver = webdriver.Chrome("chromedriver.exe")
#
#     def test_search_in_python_org(self):
#         driver = self.driver
#         driver.get("https://www.python.org")
#         self.assertIn("Python", driver.title)
#         elem = driver.find_element_by_name("q")
#         elem.send_keys("getting started with python")
#         elem.send_keys(Keys.RETURN)
#         print(driver.current_url)
#         assert "https://www.python.org/search/?q=getting+started+with+python&submit=" == driver.current_url
#
#     def tearDown(self):
#         self.driver.close()
#
#
# if __name__ == "__main__":
#     unittest.main()




