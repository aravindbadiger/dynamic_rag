import os
import json
from confluent_kafka import Consumer, Producer
import numpy as np
import qdrant_utils, config

# Kafka topics
SEARCH_REQUEST_TOPIC = os.environ.get('KAFKA_SEARCH_REQUEST_TOPIC', 'vector-search-request')
SEARCH_RESPONSE_TOPIC = os.environ.get('KAFKA_SEARCH_RESPONSE_TOPIC', 'vector-search-response')

# Consumer group for search
SEARCH_CONSUMER_GROUP = os.environ.get('KAFKA_SEARCH_CONSUMER_GROUP', 'qdrant-search-consumer-group')


def process_search_request(msg):
    try:
        req = json.loads(msg.value())
        query_vector = req['query_vector']
        top_k = req.get('top_k')
        score_threshold = req.get('score_threshold')
        collection_name = req.get('collection_name')
        request_id = req.get('request_id')
        # Perform search
        results = qdrant_utils.search(
            query_vector=query_vector,
            top_k=top_k,
            score_threshold=score_threshold,
            collection_name=collection_name,
        )
        # Send response
        response = {
            'request_id': request_id,
            'results': results,
        }
        producer = Producer({'bootstrap.servers': os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')})
        producer.produce(SEARCH_RESPONSE_TOPIC, value=json.dumps(response))
        producer.flush()
        print(f"[QdrantSearchConsumer] Responded to request_id={request_id} with {len(results)} results.")
    except Exception as e:
        print(f"[QdrantSearchConsumer] Error processing search request: {e}")


def main():
    consumer = Consumer({
        'bootstrap.servers': os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092'),
        'group.id': SEARCH_CONSUMER_GROUP,
        'auto.offset.reset': 'earliest',
    })
    consumer.subscribe([SEARCH_REQUEST_TOPIC])
    print(f"[QdrantSearchConsumer] Listening for search requests on topic: {SEARCH_REQUEST_TOPIC}")
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"[QdrantSearchConsumer] Consumer error: {msg.error()}")
                continue
            process_search_request(msg)
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()

if __name__ == "__main__":
    main()
