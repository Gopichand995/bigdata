import sys

value = 10

try:
    x = value / 0
except Exception as err_:
    print(str(err_))
    err = sys.exc_info()
    print(err)
    error = {
        "errorType": str(err[0])[8:][:-2],
        "errorOn": str(err[1])[:]
    }
    print(error)
