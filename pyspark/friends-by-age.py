from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("FriendsByAge")
sc = SparkContext(conf=conf)


def parse_line(line):
    fields = line.split(',')
    age = int(fields[2])
    num_friends = int(fields[3])
    return age, num_friends


lines = sc.textFile("file:///C:/Users/04647U744/Documents/Github/pyspark/fakefriends.csv")
rdd = lines.map(parse_line)
totals_by_age = rdd.mapValues(lambda x: (x, 1)).reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))
averagesByAge = totals_by_age.mapValues(lambda x: int(x[0] / x[1]))
results = averagesByAge.collect()
print("#############@@@@@@@@@@@@@@@@@@@*****************")
for result in results:
    print(result)
print("#############@@@@@@@@@@@@@@@@@@@*****************")
