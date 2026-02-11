"""
Qdrant vector database utilities.

Provides helpers to:
  • connect to Qdrant (remote server or local on-disk storage)
  • ensure a collection exists with the correct schema
  • upsert embedded chunks (streaming-friendly)
  • search by vector similarity
"""
from __future__ import annotations

import logging
import uuid
from typing import Any, Dict, Iterable, List, Optional, Tuple

import numpy as np
from qdrant_client import QdrantClient
from qdrant_client.http.models import (
    Distance,
    PointStruct,
    VectorParams,
    Filter,
    FieldCondition,
    MatchValue,
)

import config

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Client singleton
# ---------------------------------------------------------------------------
_client: QdrantClient | None = None


def get_client() -> QdrantClient:
    """Return a cached QdrantClient instance."""
    global _client
    if _client is not None:
        return _client

    if config.USE_QDRANT_SERVER:
        logger.info("Connecting to Qdrant server at %s:%d", config.QDRANT_HOST, config.QDRANT_PORT)
        _client = QdrantClient(host=config.QDRANT_HOST, port=config.QDRANT_PORT)
    else:
        storage_path = str(config.QDRANT_STORAGE_DIR)
        logger.info("Using local Qdrant storage at %s", storage_path)
        _client = QdrantClient(path=storage_path)

    return _client


# ---------------------------------------------------------------------------
# Collection management
# ---------------------------------------------------------------------------

def ensure_collection(
    collection_name: str | None = None,
    vector_size: int | None = None,
    distance: Distance = Distance.COSINE,
    recreate: bool = False,
) -> None:
    """Create the collection if it doesn't exist (or recreate it)."""
    client = get_client()
    collection_name = collection_name or config.QDRANT_COLLECTION
    vector_size = vector_size or config.EMBEDDING_DIM

    existing = [c.name for c in client.get_collections().collections]

    if collection_name in existing:
        if recreate:
            logger.warning("Recreating collection '%s'", collection_name)
            client.delete_collection(collection_name)
        else:
            logger.info("Collection '%s' already exists", collection_name)
            return

    logger.info(
        "Creating collection '%s' (dim=%d, distance=%s)",
        collection_name, vector_size, distance.name,
    )
    client.create_collection(
        collection_name=collection_name,
        vectors_config=VectorParams(size=vector_size, distance=distance),
    )


def collection_info(collection_name: str | None = None) -> Dict[str, Any]:
    """Return basic stats about the collection."""
    client = get_client()
    collection_name = collection_name or config.QDRANT_COLLECTION
    info = client.get_collection(collection_name)
    return {
        "name": collection_name,
        "points_count": getattr(info, "points_count", 0),
        # 'vectors_count' removed: not present in recent Qdrant versions
        "status": info.status.name if getattr(info, "status", None) else "unknown",
        "vector_size": getattr(getattr(getattr(info, "config", None), "params", None), "vectors", None).size
        if hasattr(getattr(getattr(info, "config", None), "params", None), "vectors") and hasattr(getattr(getattr(info, "config", None), "params", None).vectors, "size")
        else config.EMBEDDING_DIM,
    }


def delete_collection(collection_name: str | None = None) -> None:
    """Delete a collection."""
    client = get_client()
    collection_name = collection_name or config.QDRANT_COLLECTION
    client.delete_collection(collection_name)
    logger.info("Deleted collection '%s'", collection_name)


# ---------------------------------------------------------------------------
# Upsert
# ---------------------------------------------------------------------------

def upsert_chunks(
    chunk_vector_pairs: Iterable[Tuple[Any, np.ndarray]],
    collection_name: str | None = None,
    batch_size: int = 100,
) -> int:
    """
    Upsert an iterable of (Chunk, vector) pairs into Qdrant.

    Streams through the iterable in batches for memory efficiency.
    Returns the total number of points upserted.
    """
    client = get_client()
    collection_name = collection_name or config.QDRANT_COLLECTION

    points_buffer: List[PointStruct] = []
    total = 0

    for chunk, vector in chunk_vector_pairs:
        point = PointStruct(
            id=str(uuid.uuid5(uuid.NAMESPACE_URL, chunk.chunk_id)),
            vector=vector.tolist() if isinstance(vector, np.ndarray) else vector,
            payload={
                "text": chunk.text,
                "chunk_id": chunk.chunk_id,
                "chunk_index": chunk.chunk_index,
                "source_file": chunk.source_file,
                **chunk.metadata,
            },
        )
        points_buffer.append(point)

        if len(points_buffer) >= batch_size:
            client.upsert(collection_name=collection_name, points=points_buffer)
            total += len(points_buffer)
            logger.debug("Upserted batch of %d points (total=%d)", len(points_buffer), total)
            points_buffer.clear()

    # Flush remaining
    if points_buffer:
        client.upsert(collection_name=collection_name, points=points_buffer)
        total += len(points_buffer)
        logger.debug("Upserted final batch of %d points (total=%d)", len(points_buffer), total)

    logger.info("Upserted %d points into '%s'", total, collection_name)
    return total


# ---------------------------------------------------------------------------
# Search
# ---------------------------------------------------------------------------

def search(
    query_vector: np.ndarray | list,
    collection_name: str | None = None,
    top_k: int | None = None,
    score_threshold: float | None = None,
    source_filter: str | None = None,
) -> List[Dict[str, Any]]:
    """
    Search the collection by vector similarity.

    Returns a list of dicts with keys: id, score, text, source_file, metadata.
    """
    client = get_client()
    collection_name = collection_name or config.QDRANT_COLLECTION
    top_k = top_k or config.TOP_K_RESULTS
    score_threshold = score_threshold if score_threshold is not None else config.SEARCH_THRESHOLD

    if isinstance(query_vector, np.ndarray):
        query_vector = query_vector.tolist()

    query_filter = None
    if source_filter:
        query_filter = Filter(
            must=[FieldCondition(key="source_file", match=MatchValue(value=source_filter))]
        )

    results = client.query_points(
        collection_name=collection_name,
        query=query_vector,
        limit=top_k,
        score_threshold=score_threshold,
        query_filter=query_filter,
    )

    return [
        {
            "id": str(hit.id),
            "score": hit.score,
            "text": hit.payload.get("text", ""),
            "source_file": hit.payload.get("source_file", ""),
            "metadata": {
                k: v
                for k, v in hit.payload.items()
                if k not in ("text", "source_file")
            },
        }
        for hit in results.points
    ]
