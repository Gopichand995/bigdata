from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

import time
import os

# os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0'
# .config("spark.jars",
# "C:/Users/04647U744/Anaconda3/Lib/site-packages/pyspark/lib/jsr166e-1.1.0.jar:
# C:/Users/04647U744/Anaconda3/Lib/site-packages/pyspark/lib/spark-sql-kafka-0-10_2.12-2.4.0.jar:
# C:/Users/04647U744/Anaconda3/Lib/site-packages/pyspark/lib/kafka-clients-1.1.0.jar") \

# .config("spark.executor.extraClassPath",
# "C:/Users/04647U744/Anaconda3/Lib/site-packages/pyspark/lib/jsr166e-1.1.0.jar:
# C:/Users/04647U744/Anaconda3/Lib/site-packages/pyspark/lib/spark-sql-kafka-0-10_2.12-2.4.0.jar:
# C:/Users/04647U744/Anaconda3/Lib/site-packages/pyspark/lib/kafka-clients-1.1.0.jar") \

# .config("spark.executor.extraLibrary",
# "C:/Users/04647U744/Anaconda3/Lib/site-packages/pyspark/lib/jsr166e-1.1.0.jar:
# C:/Users/04647U744/Anaconda3/Lib/site-packages/pyspark/lib/spark-sql-kafka-0-10_2.12-2.4.0.jar:
# C:/Users/04647U744/Anaconda3/Lib/site-packages/pyspark/lib/kafka-clients-1.1.0.jar") \

# .config("spark.driver.extraClassPath",
# "C:/Users/04647U744/Anaconda3/Lib/site-packages/pyspark/lib/jsr166e-1.1.0.jar:
# C:/Users/04647U744/Anaconda3/Lib/site-packages/pyspark/lib/spark-sql-kafka-0-10_2.12-2.4.0.jar:
# C:/Users/04647U744/Anaconda3/Lib/site-packages/pyspark/lib/kafka-clients-1.1.0.jar") \

KAFKA_TOPIC_NAME = "house1"
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
DATA_FILE_PATH = "C:/Users/04647U744/Documents/Github/pyspark/realestate.csv"

CASSANDRA_HOST = "localhost"
CASSANDRA_PORT = "9042"
CASSANDRA_KEYSPACE = "realestate"
CASSANDRA_TABLE = "house"


def save_to_cassandra(df_, epoch_id):
    print(f"Printing epoch id: {epoch_id}")

    print(f"Printing before cassandra table save: {epoch_id}")

    df_.write.format("org.apache.sql.cassandra").mode("append").options(table=CASSANDRA_TABLE,
                                                                        keyspace=CASSANDRA_KEYSPACE).save()
    print(f"Printing after cassandra table save: {epoch_id}")


if __name__ == "__main__":
    print("Welcome to Pyspark Data Processing Application")
    print(time.strftime("%y-%m-%d %H:%M:%S"))

    spark = SparkSession.builder.appName("Pyspark Structured Streaming with Kafka & Cassandra").master("local[*]") \
        .config("spark.cassandra.connection.host", CASSANDRA_HOST) \
        .config("spark.cassandra.connection.port", CASSANDRA_PORT) \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    df = spark.readStream.format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", KAFKA_TOPIC_NAME) \
        .option("startingOffsets", "earliest") \
        .load()
    print(f"Printing the Schema of df: \n {df.printSchema()}")

    df1 = df.selectExpr("CAST(value as STRING)", "timestamp")

    house_schema = StructType() \
        .add("no", IntegerType()) \
        .add("timestamp", StringType()) \
        .add("houseage", IntegerType()) \
        .add("distancetomrt", IntegerType()) \
        .add("numberconveniencestores", IntegerType()) \
        .add("latitude", IntegerType()) \
        .add("longitude", IntegerType()) \
        .add("priceofunitarea", IntegerType())

    df2 = df1.select(from_json(col("value"), house_schema).alias("house"), "timestamp")
    df3 = df2.select("house.*", "timestamp")
    df3.writeStream.trigger(processingTime='15 seconds').outputMode("update").foreachBatch(save_to_cassandra).start()

    spark.stop()
