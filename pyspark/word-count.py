from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf=conf)

input_ = sc.textFile("file:///C:/Users/04647U744/Documents/Github/pyspark/book.txt")
words = input_.flatMap(lambda x: x.split())
wordCounts = words.countByValue()
for word, count in wordCounts.items():
    cleanWord = word.encode('ascii', 'ignore')
    if cleanWord and count > 10:
        print(cleanWord.decode() + " " + str(count))
