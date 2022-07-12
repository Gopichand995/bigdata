import unittest


class Test(unittest.TestCase):
    def testAssert(self):
        driver = None
        self.assertIsNotNone(driver)


if __name__ == "__main__":
    unittest.main()
