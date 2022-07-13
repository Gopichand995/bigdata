from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext

# from pyspark.streaming.kafka import KafkaUtils


KAFKA_TOPIC_NAME = "streaming1"
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
# DATA_FILE_PATH = "C:/Users/04647U744/Documents/Github/pyspark/realestate.csv"

CASSANDRA_HOST = "localhost"
CASSANDRA_PORT = "9042"
CASSANDRA_KEYSPACE = "realestate"
CASSANDRA_TABLE = "house"

if __name__ == "__main__":
    spark = SparkSession.builder.appName("Pyspark Structured Streaming with Kafka & Cassandra").master("local[*]") \
        .config("spark.cassandra.connection.host", CASSANDRA_HOST) \
        .config("spark.cassandra.connection.port", CASSANDRA_PORT) \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    df = spark.readStream.format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", KAFKA_TOPIC_NAME) \
        .option("startingOffsets", "latest") \
        .load()

    print(f"df: \n {df}")

    # sc = SparkContext(appName="Streaming Word Count")
    ssc = StreamingContext(spark.sparkContext, 60)
    # message = KafkaUtils.createDirectStream(ssc, topics=["streaming1"],
    #                                         kafkaParams={"metadata.broker.list": "localhost:9092"})
    # words = df.map(lambda x: x[1]).flatMap(lambda x: x.split(" "))
    # word_count = words.map(lambda x: (x, 1)).reduceByKey(lambda x, y: x + y)
    # word_count.pprint()

    ssc.start()
    ssc.awaitTermination()

    # spark.stop()
