from pyspark import SparkConf, SparkContext

conf = SparkConf()
sc = SparkContext(conf=conf)
n = sc.parallelize([1, 2, 3, 4])
sq = n.map(lambda x: x * 2).collect()
print(sq)