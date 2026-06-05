import json
import os

from kafka import KafkaConsumer

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "broker1:29092,broker2:29093")
TOPIC = os.getenv("CONSUME_TOPIC", "trips-topic1")
GROUP = os.getenv("GROUP_ID", "lab4-consumer")


def main():
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=BOOTSTRAP.split(","),
        group_id=GROUP,
        auto_offset_reset="earliest",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        consumer_timeout_ms=30_000,
    )
    print(f"Listening on {TOPIC} (group={GROUP})...")
    for record in consumer:
        print(
            f"partition={record.partition} offset={record.offset} "
            f"trip_id={record.value.get('trip_id')} "
            f"date={record.value.get('start_time','')[:10]}"
        )
    print("Consumer finished.")


if __name__ == "__main__":
    main()
