import logging
logging.basicConfig(level=logging.DEBUG)
from data_source import scan_directory, SUPPORTED_EXTENSIONS
import os
import json
from confluent_kafka import Producer
from data_source import DirectoryWatcher
from status_schema import make_status

def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed for {msg.key()}: {err}")
    else:
        print(f"Produced event to topic {msg.topic()}: key={msg.key()} value={msg.value()}")

def main():
    def rescan_and_stream():
        print("[FS Producer] Rescan triggered: scanning content directory...")
        for fe in scan_directory(content_dir, SUPPORTED_EXTENSIONS):
            file_path = str(fe.path)
            print(f"[FS Producer] Rescan found: {file_path}")
            try:
                mtime = os.path.getmtime(file_path)
            except Exception as e:
                print(f"[FS Producer] Could not get mtime for {file_path}: {e}")
                continue
            prev_mtime = already_ingested.get(file_path)
            if prev_mtime is not None and mtime == prev_mtime:
                print(f"[FS Producer] Rescan: file already ingested and unchanged: {file_path}")
                continue
            print(f"[FS Producer] Rescan: file ingested: {file_path}")
            already_ingested[file_path] = mtime
            with open(INGESTED_RECORD, 'w') as f:
                json.dump(already_ingested, f)
            status_msg = make_status(
                producer_id,
                'processing',
                file_path=file_path,
                details=f"Rescan detected file: {fe.path.name} (queued for ingestion)"
            )
            producer.produce(status_topic, value=json.dumps(status_msg))
            # Read file content and send in message
            try:
                with open(file_path, "r", encoding="utf-8") as f:
                    file_content = f.read()
            except Exception as e:
                print(f"[FS Producer] Could not read file {file_path}: {e}")
                continue
            # Chunk file content to fit within Kafka message size limits
            # Default Kafka max.message.bytes is 1MB (1048576 bytes), use a safe margin
            MAX_KAFKA_MSG_SIZE = int(os.environ.get("KAFKA_MAX_MSG_SIZE", 900_000))
            # Estimate JSON overhead and keep chunk smaller
            CHUNK_SIZE = MAX_KAFKA_MSG_SIZE - 4096
            # Split content into chunks
            for i in range(0, len(file_content), CHUNK_SIZE):
                chunk = file_content[i:i+CHUNK_SIZE]
                msg = json.dumps({
                    "file_name": os.path.basename(file_path),
                    "file_path": file_path,
                    "file_content": chunk,
                    "chunk_index": i // CHUNK_SIZE,
                    "num_chunks": (len(file_content) + CHUNK_SIZE - 1) // CHUNK_SIZE
                })
                print(f"[FS Producer] Sending chunk {i // CHUNK_SIZE + 1}/" \
                      f"{(len(file_content) + CHUNK_SIZE - 1) // CHUNK_SIZE} for {file_path}")
                try:
                    producer.produce(topic, value=msg, callback=delivery_report)
                    producer.poll(0)
                except Exception as e:
                    print(f"[FS Producer] Kafka produce error for chunk {i // CHUNK_SIZE + 1}: {e}")
    producer = Producer({'bootstrap.servers': os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')})
    topic = os.environ.get('KAFKA_FILE_TOPIC', 'file-system-topic')
    status_topic = os.environ.get('INGESTION_STATUS_TOPIC', 'ingestion-status-topic')
    watch_dir = os.environ.get('WATCH_DIR', './content')
    producer_id = os.environ.get('PRODUCER_ID', 'fs-producer-1')
    watcher = DirectoryWatcher(directory=watch_dir, scan_existing=True)
    watcher.start()
    print(f"Watching directory: {watch_dir} and producing to topic: {topic}")
    # Send initial running status
    status_msg = make_status(producer_id, 'running', details='File system producer started and watching for new files')
    producer.produce(status_topic, value=json.dumps(status_msg))
    producer.flush()
    try:
        INGESTED_RECORD = os.environ.get('INGESTED_RECORD', './data/ingested_files.json')
        content_dir = os.environ.get('WATCH_DIR', './content')
        # Track ingested files with their modification times
        if os.path.exists(INGESTED_RECORD):
            with open(INGESTED_RECORD, 'r') as f:
                already_ingested = json.load(f)
        else:
            already_ingested = {}

        from confluent_kafka import Consumer
        consumer = Consumer({
            'bootstrap.servers': os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092'),
            'group.id': 'fs-producer-rescan-group',
            'auto.offset.reset': 'earliest',
        })
        consumer.subscribe([topic])

        while True:
            # Check for rescan event
            msg = consumer.poll(0.1)
            if msg is not None and not msg.error():
                try:
                    payload = json.loads(msg.value())
                    if payload.get('event_type') == 'rescan':
                        print("[FS Producer] Received rescan event from Kafka, triggering rescan_and_stream...")
                        rescan_and_stream()
                        continue
                except Exception as e:
                    print(f"[FS Producer] Error parsing Kafka message: {e}")

            # DirectoryWatcher events
            try:
                event = next(watcher)
            except StopIteration:
                continue
            file_path = str(event.path)
            print(f"[FS Producer] Detected event: {event.event_type} — {file_path}")
            # Only consider files inside the content directory
            abs_file_path = os.path.abspath(file_path)
            abs_content_dir = os.path.abspath(content_dir)
            if not abs_file_path.startswith(abs_content_dir):
                print(f"[FS Producer] Skipping file outside content dir: {file_path}")
                continue
            try:
                mtime = os.path.getmtime(file_path)
            except Exception as e:
                print(f"[FS Producer] Could not get mtime for {file_path}: {e}")
                continue
            prev_mtime = already_ingested.get(file_path)
            # Only process if new or modified
            if prev_mtime is not None and mtime == prev_mtime:
                print(f"[FS Producer] File already ingested and unchanged: {file_path}")
                continue
            print(f"[FS Producer] File ingested ({event.event_type}): {file_path}")
            already_ingested[file_path] = mtime
            # Update persistent record
            with open(INGESTED_RECORD, 'w') as f:
                json.dump(already_ingested, f)
            # Send detected file status for new or modified files in content folder
            status_msg = make_status(
                producer_id,
                'processing',
                file_path=file_path,
                details=f"Detected {event.event_type} file: {event.path.name} (queued for ingestion)"
            )
            producer.produce(status_topic, value=json.dumps(status_msg))
            # Read file content and send in message
            try:
                with open(file_path, "r", encoding="utf-8") as f:
                    file_content = f.read()
            except Exception as e:
                print(f"[FS Producer] Could not read file {file_path}: {e}")
                continue
            # Chunk file content to fit within Kafka message size limits
            MAX_KAFKA_MSG_SIZE = int(os.environ.get("KAFKA_MAX_MSG_SIZE", 900000))
            CHUNK_SIZE = MAX_KAFKA_MSG_SIZE - 4096
            print(f"[FS Producer] Chunking file: {file_path} with MAX_KAFKA_MSG_SIZE={MAX_KAFKA_MSG_SIZE}, CHUNK_SIZE={CHUNK_SIZE}")
            num_chunks = (len(file_content) + CHUNK_SIZE - 1) // CHUNK_SIZE
            for i in range(0, len(file_content), CHUNK_SIZE):
                chunk = file_content[i:i+CHUNK_SIZE]
                chunk_index = i // CHUNK_SIZE
                msg = json.dumps({
                    "file_name": os.path.basename(file_path),
                    "file_path": file_path,
                    "file_content": chunk,
                    "chunk_index": chunk_index,
                    "num_chunks": num_chunks
                })
                print(f"[FS Producer] Sending chunk {chunk_index+1}/{num_chunks} for {file_path}")
                try:
                    producer.produce(topic, value=msg, callback=delivery_report)
                    producer.poll(0)
                except Exception as e:
                    print(f"[FS Producer] Kafka produce error for chunk {chunk_index+1}: {e}")
    except KeyboardInterrupt:
        print("Stopping file system producer...")
        status_msg = make_status(producer_id, 'stopped', details='Producer stopped by user')
        producer.produce(status_topic, value=json.dumps(status_msg))
        producer.flush()
    except Exception as e:
        print(f"Producer error: {e}")
        status_msg = make_status(producer_id, 'error', error=str(e))
        producer.produce(status_topic, value=json.dumps(status_msg))
        producer.flush()
    finally:
        watcher.stop()
        producer.flush()

if __name__ == "__main__":
    main()
