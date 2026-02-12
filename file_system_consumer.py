import os
import json
from confluent_kafka import Consumer
import embeddings, chunking, qdrant_utils

def process_message(msg):
    data = json.loads(msg.value())
    file_name = data.get("file_name")
    file_path = data.get("file_path")
    file_content = data.get("file_content")
    if not file_content:
        print("No file_content in message, skipping.")
        return
    # Process file content directly
    try:
        chunks = chunking.chunk_text(file_content, source_file=file_path or file_name)
        chunk_vector_stream = embeddings.embed_chunks(chunks)
        try:
            count = qdrant_utils.upsert_chunks(chunk_vector_stream)
        except Exception as e:
            # If collection does not exist, create it and retry
            if "doesn't exist" in str(e) or "Not found" in str(e):
                print(f"Collection missing, creating collection and retrying: {e}")
                qdrant_utils.ensure_collection()
                chunk_vector_stream = embeddings.embed_chunks(chunking.chunk_text(file_content, source_file=file_path or file_name))
                count = qdrant_utils.upsert_chunks(chunk_vector_stream)
            else:
                raise
        print(f"Upserted {count} chunks from Kafka message: {file_path or file_name}")
    except Exception as e:
        print(f"Error processing Kafka message ({file_path or file_name}): {e}")

def main():
    consumer = Consumer({
        'bootstrap.servers': os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092'),
        'group.id': os.environ.get('KAFKA_CONSUMER_GROUP', 'file-system-consumer-group'),
        'auto.offset.reset': 'earliest',
    })
    topic = os.environ.get('KAFKA_FILE_TOPIC', 'file-system-topic')
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
