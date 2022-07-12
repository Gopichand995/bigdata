import unittest


class SignUpTest(unittest.TestCase):
    def test_signbygmail(self):
        print("This is sign by gmail test")
        self.assertTrue(True)

    def test_signbyfacebook(self):
        print("This is sign by facebook test")
        self.assertTrue(True)

    def test_signbytwitter(self):
        print("This is sign by twitter test")
        self.assertTrue(True)


if __name__ == "__main__":
    unittest.main()
