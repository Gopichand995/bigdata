import unittest
from selenium import webdriver


class SearchEnginesTest(unittest.TestCase):
    def test_google_chrome(self):
        self.driver = webdriver.Chrome("C:\\Users\\GopichandBarri\\Documents\\Github\\peace\\chromedriver.exe")
        self.driver.get("https://www.google.com/")
        print("Title of the page is: {}".format(self.driver.title))
        self.driver.close()


if __name__ == "__main__":
    unittest.main()
