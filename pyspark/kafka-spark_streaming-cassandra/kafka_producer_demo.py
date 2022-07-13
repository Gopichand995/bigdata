import time

from kafka import KafkaProducer
from json import dumps
import pandas as pd

KAFKA_TOPIC_NAME = "house1"
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"

if __name__ == "__main__":
    print("Kafka Producer Application Started....")

    kafka_prod_obj = KafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                                   api_version=(0, 11, 5),
                                   value_serializer=lambda x: dumps(x).encode("utf-8"))
    file_path = "realestate1.csv"
    house_df = pd.read_csv(file_path)
    print(house_df.head())

    house_list = house_df.to_dict(orient="records")
    print(house_list[0])

    for house in house_list:
        message = house
        print(f"Message to be print: {message}")
        kafka_prod_obj.send(KAFKA_TOPIC_NAME, message)
        time.sleep(.5)
