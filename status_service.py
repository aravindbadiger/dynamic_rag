import os
import json
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from confluent_kafka import Consumer
from status_schema import IngestionStatus


STATUS_FILE = os.environ.get('STATUS_FILE', '/app/data/ingestion_status.json')
KAFKA_BOOTSTRAP_SERVERS = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
STATUS_TOPIC = os.environ.get('INGESTION_STATUS_TOPIC', 'ingestion-status-topic')
QDRANT_STATS_TOPIC = os.environ.get('QDRANT_STATS_TOPIC', 'qdrant-stats-topic')
CONSUMER_GROUP = os.environ.get('STATUS_CONSUMER_GROUP', 'status-service-group')

latest_status = {}
latest_qdrant_stats = {}


class StatusHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == '/status':
            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            with threading.Lock():
                self.wfile.write(json.dumps(latest_status).encode())
        elif self.path == '/qdrant-status':
            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            with threading.Lock():
                self.wfile.write(json.dumps(latest_qdrant_stats).encode())
        else:
            self.send_response(404)
            self.end_headers()


def kafka_status_consumer():
    global latest_status, latest_qdrant_stats
    consumer = Consumer({
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'group.id': CONSUMER_GROUP,
        'auto.offset.reset': 'latest',
    })
    consumer.subscribe([STATUS_TOPIC, QDRANT_STATS_TOPIC])
    print(f"[StatusService] Listening for status on topics: {STATUS_TOPIC}, {QDRANT_STATS_TOPIC}")
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"[StatusService] Consumer error: {msg.error()}")
                continue
            try:
                topic = msg.topic()
                if topic == STATUS_TOPIC:
                    status = json.loads(msg.value())
                    producer_id = status.get('producer_id', 'unknown')
                    with threading.Lock():
                        latest_status[producer_id] = status
                        # Persist to file for durability
                        with open(STATUS_FILE, 'w') as f:
                            json.dump(latest_status, f)
                elif topic == QDRANT_STATS_TOPIC:
                    stats_msg = json.loads(msg.value())
                    stats = stats_msg.get('stats', {})
                    with threading.Lock():
                        if not stats:
                            latest_qdrant_stats = {}
                        else:
                            latest_qdrant_stats = stats
            except Exception as e:
                print(f"[StatusService] Error processing message: {e}")
    finally:
        consumer.close()

def run_http_server():
    server_address = ('', 8080)
    httpd = HTTPServer(server_address, StatusHandler)
    print("[StatusService] HTTP server running on port 8080 (/status)")
    httpd.serve_forever()

def main():
    t = threading.Thread(target=kafka_status_consumer, daemon=True)
    t.start()
    run_http_server()

if __name__ == "__main__":
    main()
