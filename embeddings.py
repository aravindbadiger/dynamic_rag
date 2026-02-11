"""
Embedding module — generates vector embeddings using sentence-transformers.

Provides a lazy-loaded singleton so the model is only downloaded/loaded once
and can be shared across the streaming pipeline.
"""
from __future__ import annotations

import logging
from typing import List

import numpy as np
from sentence_transformers import SentenceTransformer

import config

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Singleton model cache
# ---------------------------------------------------------------------------
_model: SentenceTransformer | None = None


def get_model() -> SentenceTransformer:
    """Return the cached SentenceTransformer model (lazy-loaded)."""
    global _model
    if _model is None:
        logger.info("Loading embedding model: %s", config.EMBEDDING_MODEL)
        _model = SentenceTransformer(config.EMBEDDING_MODEL)
        logger.info(
            "Model loaded — dimension=%d", _model.get_sentence_embedding_dimension()
        )
    return _model


def get_dimension() -> int:
    """Return the embedding dimension for the configured model."""
    return config.EMBEDDING_DIM


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def embed_text(text: str) -> np.ndarray:
    """Embed a single text string → 1-D numpy array."""
    model = get_model()
    return model.encode(text, convert_to_numpy=True, show_progress_bar=False)


def embed_texts(texts: List[str], batch_size: int = 64) -> np.ndarray:
    """
    Embed a list of texts → 2-D numpy array (N × dim).

    Uses batching internally for efficiency.
    """
    model = get_model()
    return model.encode(
        texts,
        batch_size=batch_size,
        convert_to_numpy=True,
        show_progress_bar=False,
    )


def embed_chunks(chunks, batch_size: int = 64):
    """
    Accept an iterable of Chunk objects, embed their text, and yield
    (chunk, vector) tuples.

    This is the streaming-friendly API — it batches internally but yields
    results as soon as each batch completes.
    """
    batch_chunks = []
    batch_texts = []

    for chunk in chunks:
        batch_chunks.append(chunk)
        batch_texts.append(chunk.text)

        if len(batch_texts) >= batch_size:
            vectors = embed_texts(batch_texts, batch_size=batch_size)
            for c, v in zip(batch_chunks, vectors):
                yield c, v
            batch_chunks.clear()
            batch_texts.clear()

    # Flush remaining
    if batch_texts:
        vectors = embed_texts(batch_texts, batch_size=batch_size)
        for c, v in zip(batch_chunks, vectors):
            yield c, v
