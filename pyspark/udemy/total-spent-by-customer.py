from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("SpendByCustomer")
sc = SparkContext(conf=conf)


def extractCustomerPricePairs(line):
    fields = line.split(',')
    return int(fields[0]), float(fields[2])


input_ = sc.textFile("file:///C:/Users/04647U744/Documents/Github/pyspark/customer-orders.csv")
mappedInput = input_.map(extractCustomerPricePairs)
totalByCustomer = mappedInput.reduceByKey(lambda x, y: x + y).sortByKey()
# totalByCustomer2 = totalByCustomer.map(lambda x: (x[1], "{:.2f}".format(x[0])))

results = totalByCustomer.collect()
for result in results:
    print(result)
