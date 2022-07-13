from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql import functions as func

spark = SparkSession.builder.appName("FriendsByAge").getOrCreate()

lines = spark.read.option("header", "true").option("inferSchema", "true").csv(
    "file:///C:/Users/04647U744/Documents/Github/pyspark/fakefriends-header.csv")

# Select only age and numFriends columns
friendsByAge = lines.select("age", "friends")

# From friendsByAge we group by "age" and then compute average
friendsByAge.groupBy("age").avg("friends").show()

# Sorted
friendsByAge.groupBy("age").avg("friends").sort("age").show()

# Formatted more nicely
friendsByAge.groupBy("age").agg(func.round(func.avg("friends"))).sort("age").show()

# With a custom column name
friendsByAge.groupBy("age").agg(func.round(func.avg("friends")).alias("friends_avg")).sort("age").show()

spark.stop()
