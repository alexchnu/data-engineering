import csv
import json
import os
import time

from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "broker1:29092,broker2:29093")
TOPICS = os.getenv("TOPICS", "trips-topic1,trips-topic2").split(",")
CSV_PATH = os.getenv("CSV_PATH", "/data/Divvy_Trips_2019_Q4.csv")
MAX_ROWS = int(os.getenv("MAX_ROWS", "0"))


def wait_for_kafka(bootstrap: str, retries: int = 15, delay: float = 4.0) -> KafkaProducer:
    servers = bootstrap.split(",")
    for attempt in range(retries):
        try:
            producer = KafkaProducer(
                bootstrap_servers=servers,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: str(k).encode("utf-8"),
                acks="all",
                retries=3,
            )
            print(f"Connected to Kafka: {servers}")
            return producer
        except NoBrokersAvailable:
            print(f"Kafka not ready, retry {attempt + 1}/{retries}...")
            time.sleep(delay)
    raise RuntimeError("Could not connect to Kafka after retries")


def main():
    producer = wait_for_kafka(BOOTSTRAP)

    sent = 0
    with open(CSV_PATH, newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            msg = {
                "trip_id": row["trip_id"],
                "start_time": row["start_time"],
                "end_time": row["end_time"],
                "bikeid": row["bikeid"],
                "tripduration": float(row["tripduration"].replace(",", "")) if row["tripduration"] else 0.0,
                "from_station_id": row["from_station_id"],
                "from_station_name": row["from_station_name"],
                "to_station_id": row["to_station_id"],
                "to_station_name": row["to_station_name"],
                "usertype": row["usertype"],
                "gender": row["gender"],
                "birthyear": row["birthyear"],
            }
            for topic in TOPICS:
                producer.send(topic, key=row["trip_id"], value=msg)
            sent += 1
            if sent % 500 == 0:
                producer.flush()
                print(f"Sent {sent} messages to {TOPICS}")
            if MAX_ROWS and sent >= MAX_ROWS:
                break

    producer.flush()
    print(f"Done. Total messages sent: {sent} to topics: {TOPICS}")


if __name__ == "__main__":
    main()
