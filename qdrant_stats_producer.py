"""
Qdrant Stats Producer

Periodically queries Qdrant collection statistics and publishes them to a Kafka topic for the status service to consume.
"""
import os
import time
import json
from confluent_kafka import Producer
import qdrant_utils
import config

KAFKA_TOPIC = os.environ.get("QDRANT_STATS_TOPIC", "qdrant-stats-topic")
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
POLL_INTERVAL = int(os.environ.get("QDRANT_STATS_POLL_INTERVAL", 30))  # seconds

producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})

def publish_stats():
    try:
        info = qdrant_utils.collection_info()
    except Exception as e:
        # If collection does not exist, send empty stats
        info = {}
        print(f"Collection not found or error: {e}, sending empty stats.")
    payload = json.dumps({
        "timestamp": int(time.time()),
        "stats": info,
    })
    producer.produce(KAFKA_TOPIC, payload)
    producer.flush()
    print(f"Published Qdrant stats: {payload}")

if __name__ == "__main__":
    while True:
        try:
            publish_stats()
        except Exception as e:
            print(f"Error publishing Qdrant stats: {e}")
        time.sleep(POLL_INTERVAL)
