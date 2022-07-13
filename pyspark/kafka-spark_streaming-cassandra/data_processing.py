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
# CASSANDRA_KEYSPACE = "realestate"
# CASSANDRA_TABLE = "house"

MYSQL_HOST = "localhost"
MYSQL_PORT = "3306"
MYSQL_DATABASE = "mysqldb1"
MYSQL_DRIVER_CLASS = "com.mysql.jdbc.Driver"
MYSQL_TABLE = "house"
MYSQL_USERNAME = "root"
MYSQL_PASSWORD = "mysql7860*"
MYSQL_JDBC_URL = "jdbc:mysql://" + MYSQL_HOST + ":" + MYSQL_PORT + "/" + MYSQL_DATABASE


def save_to_cassandra(df_, epoch_id):
    print(f"df_: \n{df_}")
    print(f"Printing epoch id: {epoch_id}")
    print(f"Printing before cassandra table save: {epoch_id}")
    df_.write.format("org.apache.spark.sql.cassandra").options(table="house", keyspace="realestate").save(mode="append")
    print(f"Printing after cassandra table save: {epoch_id}")


def save_to_mysql(df_, epoch_id):
    db_creds = {"user": MYSQL_USERNAME, "password": MYSQL_PASSWORD, "driver": MYSQL_DRIVER_CLASS}
    print(f"Printing epoch id: {epoch_id}")
    print(f"Printing before cassandra table save: {epoch_id}")
    df_.write.jdbc(url=MYSQL_JDBC_URL, table=MYSQL_TABLE, mode="append", properties=db_creds)
    print(f"Printing before cassandra table save: {epoch_id}")


if __name__ == "__main__":
    print("Welcome to Pyspark Data Processing Application")
    print(time.strftime("%y-%m-%d %H:%M:%S"))

    spark = SparkSession.builder.appName("Pyspark Structured Streaming with Kafka & Cassandra") \
        .master("local[*]") \
        .config("spark.cassandra.connection.host", CASSANDRA_HOST) \
        .config("spark.cassandra.connection.port", CASSANDRA_PORT) \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    df = spark.readStream.format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", KAFKA_TOPIC_NAME) \
        .option("startingOffsets", "earliest") \
        .load()
    df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
    # print(f"Printing the Schema of df: \n {df.printSchema()}")

    house_schema = StructType() \
        .add("no", IntegerType()) \
        .add("timestamp1", StringType()) \
        .add("houseage", StringType()) \
        .add("distancetomrt", StringType()) \
        .add("numberconveniencestores", StringType()) \
        .add("latitude", StringType()) \
        .add("longitude", StringType()) \
        .add("priceofunitarea", StringType())

    house_stream = df.select(from_json(col("value").cast("string"), house_schema).alias("parsed_house_values"))

    house_data = house_stream.select("parsed_house_values.*")

    # ingest into cassandra
    house_data.writeStream.trigger(processingTime='15 seconds').outputMode("update").foreachBatch(
        save_to_cassandra).start()

    # ingest into mysql
    house_data.writeStream.trigger(processingTime='15 seconds').outputMode("update").foreachBatch(save_to_mysql).start()
    qry = house_data.writeStream.outputMode("append").format("console").start()

    qry.awaitTermination()
