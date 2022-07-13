from pyspark import SparkContext
from kafka import KafkaProducer

import os
import time

os.environ[
    'PYSPARK_SUBMIT_ARGS'] = '--conf spark.ui.port=4040 --packages org.apache.spark:spark-streaming-kafka_2.10:1.6.3.2.4.2.0-90'

sc = SparkContext("local[1]", "KafkaSendStream")
producer = KafkaProducer(bootstrap_servers="localhost:9092", api_version=(0, 11, 5))
while True:
    message = time.strftime("%Y-%m-%d %H:%M:%S")
    print(message)
    producer.send('test', message)
    producer.flush()
    time.sleep(1)
