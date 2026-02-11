"""
Document chunking module.

Splits raw text documents into overlapping chunks suitable for embedding.
Supports configurable chunk size, minimum size, and overlap.
"""
from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Generator, List

import config


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------
@dataclass
class Chunk:
    """A single text chunk with metadata."""
    text: str
    chunk_index: int
    source_file: str
    chunk_id: str = ""
    metadata: dict = field(default_factory=dict)

    def __post_init__(self):
        if not self.chunk_id:
            # Deterministic ID from content + source so re-ingestion is idempotent
            digest = hashlib.sha256(
                f"{self.source_file}::{self.chunk_index}::{self.text[:128]}".encode()
            ).hexdigest()[:16]
            self.chunk_id = f"{Path(self.source_file).stem}_{self.chunk_index}_{digest}"


# ---------------------------------------------------------------------------
# Sentence-boundary aware splitting
# ---------------------------------------------------------------------------
_SENTENCE_RE = re.compile(r"(?<=[.!?])\s+")


def _split_sentences(text: str) -> List[str]:
    """Split text into sentences (simple regex heuristic)."""
    return [s.strip() for s in _SENTENCE_RE.split(text) if s.strip()]


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------
def chunk_text(
    text: str,
    source_file: str = "unknown",
    max_chars: int | None = None,
    min_chars: int | None = None,
    overlap: int | None = None,
    extra_metadata: dict | None = None,
) -> Generator[Chunk, None, None]:
    """
    Yield Chunk objects by splitting *text* into segments of roughly
    *max_chars* characters with *overlap* character overlap.

    The splitter prefers to break on sentence boundaries when possible.
    """
    max_chars = max_chars or config.CHUNK_MAX_CHARS
    min_chars = min_chars or config.CHUNK_MIN_CHARS
    overlap = overlap if overlap is not None else config.CHUNK_OVERLAP
    extra_metadata = extra_metadata or {}

    # Normalise whitespace
    text = re.sub(r"\s+", " ", text).strip()
    if not text:
        return

    sentences = _split_sentences(text)
    if not sentences:
        # Fallback: treat entire text as one sentence
        sentences = [text]

    buffer: list[str] = []
    buf_len = 0
    chunk_idx = 0

    for sentence in sentences:
        sent_len = len(sentence)

        # If adding this sentence would exceed max, flush buffer
        if buffer and buf_len + sent_len + 1 > max_chars:
            chunk_text_str = " ".join(buffer)
            if len(chunk_text_str) >= min_chars:
                yield Chunk(
                    text=chunk_text_str,
                    chunk_index=chunk_idx,
                    source_file=source_file,
                    metadata={
                        "char_count": len(chunk_text_str),
                        **extra_metadata,
                    },
                )
                chunk_idx += 1

            # Keep overlap: walk backwards until we have ~overlap chars
            overlap_buf: list[str] = []
            overlap_len = 0
            for s in reversed(buffer):
                if overlap_len + len(s) > overlap:
                    break
                overlap_buf.insert(0, s)
                overlap_len += len(s) + 1
            buffer = overlap_buf
            buf_len = sum(len(s) for s in buffer) + max(len(buffer) - 1, 0)

        buffer.append(sentence)
        buf_len += sent_len + (1 if buf_len else 0)

    # Flush remaining buffer
    if buffer:
        chunk_text_str = " ".join(buffer)
        if len(chunk_text_str) >= min_chars:
            yield Chunk(
                text=chunk_text_str,
                chunk_index=chunk_idx,
                source_file=source_file,
                metadata={"char_count": len(chunk_text_str), **extra_metadata},
            )


def chunk_file(filepath: str | Path, **kwargs) -> Generator[Chunk, None, None]:
    """Read a file and yield chunks."""
    path = Path(filepath)
    if not path.exists():
        raise FileNotFoundError(f"File not found: {path}")

    text = path.read_text(encoding="utf-8", errors="replace")
    yield from chunk_text(text, source_file=str(path), **kwargs)
