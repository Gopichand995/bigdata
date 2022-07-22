import os

from pyspark.sql import SparkSession
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.recommendation import ALS
from pyspark.sql import Row

# conf = SparkConf().setAppName("netflix reccommendation system").setMaster("local")
# sc = SparkContext(conf=conf)
spark = SparkSession.builder.appName('recommender_system').master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

lines = spark.read.text(os.getcwd() + "/data/ratings.txt").rdd
parts = lines.map(lambda row: row.value.split("::"))
ratings_rdd = parts.map(
    lambda p: Row(userId=int(p[0]), movieId=int(p[1]), rating=float(int(p[1])), timestamp=int(p[3])))
ratings = spark.createDataFrame(ratings_rdd)
training, test = ratings.randomSplit([0.8, 0.2])

als = ALS(maxIter=5, regParam=0.01, userCol="userId", itemCol="movieId", ratingCol="rating", coldStartStrategy="drop")
model = als.fit(training)

predictions = model.transform(test)
evaluator = RegressionEvaluator(metricName="rmse", labelCol="rating", predictionCol="prediction")
rmse = evaluator.evaluate(predictions)
print(f"Root-Mean_Square-Error = {str(rmse)}")

user_recs = model.recommendForAllUsers(10)
movie_recs = model.recommendForAllItems(10)

users = ratings.select(als.getUserCol()).distinct().limit(3)
user_subset_recs = model.recommendForUserSubset(users, 10)

movies = ratings.select(als.getItemCol()).distinct().limit(3)
movie_subset_recs = model.recommendForItemSubset(movies, 10)

user_recs.show()
movie_recs.show()
user_subset_recs.show()
movie_subset_recs.show()
