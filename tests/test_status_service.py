"""
Tests for the status_service HTTP API and status message flow.
"""
import json
import threading
import time
import tempfile
from http.server import HTTPServer
from status_service import StatusHandler, latest_status

import pytest
import requests

@pytest.fixture(scope="module")
def status_server():
    # Start the HTTP server in a background thread
    server_address = ("localhost", 8081)
    httpd = HTTPServer(server_address, StatusHandler)
    t = threading.Thread(target=httpd.serve_forever, daemon=True)
    t.start()
    # Give the server a moment to start
    time.sleep(0.5)
    yield
    httpd.shutdown()
    t.join()

def test_status_endpoint_empty(status_server):
    # Should return empty status dict
    resp = requests.get("http://localhost:8081/status", timeout=2)
    assert resp.status_code == 200
    data = resp.json()
    assert data == {}

def test_status_endpoint_with_data(status_server):
    # Simulate a status update
    latest_status["test-producer"] = {
        "producer_id": "test-producer",
        "timestamp": time.time(),
        "status": "running",
        "details": "Test run",
        "file_path": "/tmp/foo.txt",
        "error": None,
    }
    resp = requests.get("http://localhost:8081/status", timeout=2)
    assert resp.status_code == 200
    data = resp.json()
    assert "test-producer" in data
    assert data["test-producer"]["status"] == "running"
    assert data["test-producer"]["file_path"] == "/tmp/foo.txt"
