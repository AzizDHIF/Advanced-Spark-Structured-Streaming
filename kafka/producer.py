# Write your kafka producer code here

"""
Kafka Producer — reads events_dirty.json and sends each line to the
'sensor-events' topic, simulating a real-time stream.

Usage:
    python kafka/producer.py --data data/events_dirty.json --delay 0.5
"""

import argparse
import json
import time
from kafka import KafkaProducer

TOPIC = "sensor-events"
BOOTSTRAP = "localhost:9092"


def create_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=[BOOTSTRAP],
        value_serializer=lambda v: v.encode("utf-8"),  # send raw string — dirty data included
        acks="all",
        retries=3,
    )


def stream_file(filepath: str, delay: float, loop: bool) -> None:
    producer = create_producer()
    print(f"[producer] Connected. Streaming '{filepath}' → topic '{TOPIC}'")

    iteration = 0
    while True:
        iteration += 1
        with open(filepath, "r") as fh:
            lines = [line.strip() for line in fh if line.strip()]

        print(f"[producer] Pass {iteration}: sending {len(lines)} lines...")
        for line in lines:
            producer.send(TOPIC, value=line)
            print(f"  → {line[:80]}{'...' if len(line) > 80 else ''}")
            time.sleep(delay)

        producer.flush()
        print(f"[producer] Pass {iteration} complete.")

        if not loop:
            break

        print(f"[producer] Looping in 5 s...")
        time.sleep(5)

    producer.close()
    print("[producer] Done.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--data", default="data/events_dirty.json", help="Path to the JSON file")
    parser.add_argument("--delay", type=float, default=0.5, help="Seconds between messages")
    parser.add_argument("--loop", action="store_true", help="Keep looping the file forever")
    args = parser.parse_args()

    stream_file(args.data, args.delay, args.loop)