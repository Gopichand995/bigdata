import unittest
from Package1.TC_LoginTest import LoginTest
from Package1.TC_SignUpTest import SignUpTest

from Package2.TC_PaymentTest import PaymentTest
from Package2.TC_PaymentReturnsTest import PaymentReturnsTest

ts1 = unittest.TestLoader().loadTestsFromTestCase(LoginTest)
ts2 = unittest.TestLoader().loadTestsFromTestCase(SignUpTest)
ts3 = unittest.TestLoader().loadTestsFromTestCase(PaymentTest)
ts4 = unittest.TestLoader().loadTestsFromTestCase(PaymentReturnsTest)

# creating test suites
sanityTestSuite = unittest.TestSuite([ts1, ts2])
functionalTestSuite = unittest.TestSuite([ts3, ts4])
masterTestSuite = unittest.TestSuite([ts1, ts2, ts3, ts4])

# unittest.TextTestRunner().run(sanityTestSuite)
# unittest.TextTestRunner().run(functionalTestSuite)
unittest.TextTestRunner(verbosity=2).run(masterTestSuite)
