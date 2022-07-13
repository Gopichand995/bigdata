from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext, Row
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import pprint

import os
import time

os.environ[
    'PYSPARK_SUBMIT_ARGS'] = '--conf spark.ui.port=4040 --packages org.apache.spark:spark-streaming-kafka_2.10:1.6.3.2.4.2.0-90'

conf = SparkConf().setAppName("Streaming test").setMaster("local[2]").set("spark.cassandra.connection.host",
                                                                          "127.0.0.1")
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)


def save_to_cassandra(rows):
    if not rows.isEmpty():
        sqlContext.createDataFrame(rows) \
            .write.format("org.apache.spark.sql.cassandra") \
            .mode("append") \
            .options(table="house", keyspace='realestate') \
            .save()


ssc = StreamingContext(sc, 5)
kvs = KafkaUtils.createStream(ssc, "127.0.0.1:2181", "spark-streaming-consumer", {"test": 1})
data = kvs.map(lambda x: x[1])
rows = data.map(lambda x: Row(time_sent=x, time_received=time.strftime("%Y-%m-%d %H:%M:%S")))
rows.foreachRDD(save_to_cassandra)
rows.pprint()

ssc.start()

time.sleep(300)

ssc.stop(stopSparkContext=False, stopGraceFully=True)
