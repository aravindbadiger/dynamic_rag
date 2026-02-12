import os
import json
from confluent_kafka import Consumer
import embeddings, chunking, qdrant_utils

def process_message(msg):
    data = json.loads(msg.value())
    # Process document content from Kafka message
    text = data.get("text")
    source = data.get("source", "mongo")
    if not text:
        print("No text in message, skipping.")
        return
    try:
        chunks = chunking.chunk_text(text, source_file=source)
        chunk_vector_stream = embeddings.embed_chunks(chunks)
        count = qdrant_utils.upsert_chunks(chunk_vector_stream)
        print(f"Upserted {count} chunks from Kafka message: {source}")
    except Exception as e:
        print(f"Error processing Kafka message ({source}): {e}")

def main():
    consumer = Consumer({
        'bootstrap.servers': os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092'),
        'group.id': os.environ.get('KAFKA_CONSUMER_GROUP', 'mongo-db-consumer-group'),
        'auto.offset.reset': 'earliest',
    })
    topic = os.environ.get('KAFKA_MONGO_TOPIC', 'mongo-db-topic')
    consumer.subscribe([topic])
    print(f"Listening to topic: {topic}")
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"Consumer error: {msg.error()}")
                continue
            process_message(msg)
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()

if __name__ == "__main__":
    main()
