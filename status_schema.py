# Status message schema for ingestion status topic

from typing import TypedDict, Optional
import time

class IngestionStatus(TypedDict):
    producer_id: str
    timestamp: float
    status: str  # e.g., 'running', 'idle', 'error', 'processing'
    details: Optional[str]
    file_path: Optional[str]
    error: Optional[str]

def make_status(
    producer_id: str,
    status: str,
    details: Optional[str] = None,
    file_path: Optional[str] = None,
    error: Optional[str] = None,
) -> IngestionStatus:
    return {
        "producer_id": producer_id,
        "timestamp": time.time(),
        "status": status,
        "details": details,
        "file_path": file_path,
        "error": error,
    }

# Example usage:
# msg = make_status("fs-producer-1", "processing", file_path="/content/foo.txt")
