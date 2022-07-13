from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("SpendByCustomerSorted")
sc = SparkContext(conf=conf)


def extractCustomerPricePairs(line):
    fields = line.split(',')
    return int(fields[0]), round(float(fields[2]), 2)


input_ = sc.textFile("file:///C:/Users/04647U744/Documents/Github/pyspark/customer-orders.csv")
mappedInput = input_.map(extractCustomerPricePairs)
totalByCustomer = mappedInput.reduceByKey(lambda x, y: x + y)

# Changed for Python 3 compatibility:
# flipped = totalByCustomer.map(lambda (x,y):(y,x))
flipped = totalByCustomer.map(lambda x: (x[1], x[0]))

totalByCustomerSorted = flipped.sortByKey()

results = totalByCustomerSorted.collect()
for result in results:
    print(result)
