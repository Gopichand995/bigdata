import unittest


class AppTesting(unittest.TestCase):

    @unittest.SkipTest
    def test_search(self):
        print("This is search test")

    @unittest.skip("The advanced test has been skipped for corresponding reasons")
    def test_advancedsearch(self):
        print("This is advanced test")

    @unittest.skipIf(1==1, "The prepaid rechage has been skipped for the high prices")
    def test_prepaidrecharge(self):
        print("This is prepaid recharge")

    def test_postpaidrecharge(self):
        print("This is postpaid recharge")

    def test_login_by_gmail(self):
        print("This is login by gmail")

    def test_login_by_twitter(self):
        print("This is login by twitter")


if __name__ == "__main__":
    unittest.main()
