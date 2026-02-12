import os
import json
from confluent_kafka import Producer
from status_schema import make_status
# Placeholder: replace with actual MongoDB change stream logic

def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed for {msg.key()}: {err}")
    else:
        print(f"Produced event to topic {msg.topic()}: key={msg.key()} value={msg.value()}")

def main():
    producer = Producer({'bootstrap.servers': os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')})
    topic = os.environ.get('KAFKA_MONGO_TOPIC', 'mongo-db-topic')
    status_topic = os.environ.get('INGESTION_STATUS_TOPIC', 'ingestion-status-topic')
    producer_id = os.environ.get('PRODUCER_ID', 'mongo-producer-1')
    print(f"Producing MongoDB events to topic: {topic}")
    # Send initial running status
    status_msg = make_status(producer_id, 'running', details='MongoDB producer started and watching for new documents')
    producer.produce(status_topic, value=json.dumps(status_msg))
    producer.flush()
    try:
        # Kafka message size limit (default 1MB, use safe margin)
        MAX_KAFKA_MSG_SIZE = int(os.environ.get("KAFKA_MAX_MSG_SIZE", 900_000))
        CHUNK_SIZE = MAX_KAFKA_MSG_SIZE - 4096
        while True:
            # TODO: Replace with actual MongoDB change stream event detection
            # Simulate a new document event (replace this with real change stream logic)
            event = {
                "source": "mongo_collection/document_id",
                "text": "This is the content of the MongoDB document. " * 10000,  # Simulate large doc
                "metadata": {"field1": "value1"}
            }
            doc_text = event.get("text", "")
            num_chunks = (len(doc_text) + CHUNK_SIZE - 1) // CHUNK_SIZE
            # Send status for new doc
            status_msg = make_status(
                producer_id,
                'processing',
                details=f'Detected new MongoDB document in collection: {event.get("source", "unknown")} (queued for ingestion)',
                file_path=event.get('source')
            )
            producer.produce(status_topic, value=json.dumps(status_msg))
            for i in range(0, len(doc_text), CHUNK_SIZE):
                chunk = doc_text[i:i+CHUNK_SIZE]
                chunk_index = i // CHUNK_SIZE
                msg = json.dumps({
                    "source": event.get("source"),
                    "metadata": event.get("metadata", {}),
                    "chunk_index": chunk_index,
                    "num_chunks": num_chunks,
                    "text_chunk": chunk
                })
                print(f"[Mongo Producer] Sending chunk {chunk_index+1}/{num_chunks} for {event.get('source')}")
                try:
                    producer.produce(topic, value=msg, callback=delivery_report)
                    producer.poll(0)
                except Exception as e:
                    print(f"[Mongo Producer] Kafka produce error for chunk {chunk_index+1}: {e}")
            import time; time.sleep(10)  # Simulate interval
    except KeyboardInterrupt:
        print("Stopping mongo db producer...")
        status_msg = make_status(producer_id, 'stopped', details='MongoDB producer stopped by user')
        producer.produce(status_topic, value=json.dumps(status_msg))
        producer.flush()
    except Exception as e:
        print(f"MongoDB producer error: {e}")
        status_msg = make_status(producer_id, 'error', error=str(e))
        producer.produce(status_topic, value=json.dumps(status_msg))
        producer.flush()
    finally:
        producer.flush()

if __name__ == "__main__":
    main()
