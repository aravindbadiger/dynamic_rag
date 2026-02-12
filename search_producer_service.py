import os
import json
from flask import Flask, request, jsonify
from confluent_kafka import Producer, Consumer
import uuid
import time

app = Flask(__name__)

KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
SEARCH_REQUEST_TOPIC = os.environ.get("KAFKA_SEARCH_REQUEST_TOPIC", "vector-search-request")
SEARCH_RESPONSE_TOPIC = os.environ.get("KAFKA_SEARCH_RESPONSE_TOPIC", "vector-search-response")
SEARCH_CLIENT_GROUP = os.environ.get("KAFKA_SEARCH_CLIENT_GROUP", "rag-client-search-group")

@app.route("/search", methods=["POST"])
def search():
    data = request.json
    question_vector = data.get("query_vector")
    top_k = data.get("top_k")
    score_threshold = data.get("score_threshold")
    collection_name = data.get("collection_name")
    request_id = str(uuid.uuid4())
    req = {
        "request_id": request_id,
        "query_vector": question_vector,
        "top_k": top_k,
        "score_threshold": score_threshold,
        "collection_name": collection_name,
    }
    # Send search request to Kafka
    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})
    producer.produce(SEARCH_REQUEST_TOPIC, value=json.dumps(req))
    producer.flush()
    # Listen for response
    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "group.id": f"{SEARCH_CLIENT_GROUP}-{request_id}",
        "auto.offset.reset": "earliest",
    })
    consumer.subscribe([SEARCH_RESPONSE_TOPIC])
    start = time.time()
    timeout = 10.0
    results = []
    try:
        while time.time() - start < timeout:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                continue
            try:
                resp = json.loads(msg.value())
                if resp.get("request_id") == request_id:
                    results = resp.get("results", [])
                    break
            except Exception:
                continue
    finally:
        consumer.close()
    return jsonify({"results": results})

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8081)
