"""
Data ingestion pipeline — streaming architecture.

Three operating modes:
  1. ``ingest_file``   — ingest a single file end-to-end.
  2. ``ingest_scan``   — one-shot: scan content/ and ingest all files.
  3. ``ingest_watch``  — continuous: watch content/ for new files and
                         stream them through chunk → embed → upsert.

The core pipeline function ``_stream_file`` is shared by all modes:

    file → extract_text → chunk_text → embed_chunks → upsert_chunks

CLI usage:
    python ingest_data.py scan              # one-shot bulk ingest
    python ingest_data.py watch             # continuous watcher
    python ingest_data.py file <path>       # ingest a single file
"""
from __future__ import annotations

import argparse
import json
import logging
import signal
import sys
import time
from pathlib import Path
from typing import Optional

import chunking
import embeddings
import qdrant_utils
from data_source import DirectoryWatcher, FileEvent, extract_text, scan_directory

import config

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger(__name__)

# Track what has already been ingested (path → mtime) so we skip duplicates
_ingested_files: dict[str, float] = {}


# ---------------------------------------------------------------------------
# Core streaming pipeline: file → chunks → embeddings → Qdrant
# ---------------------------------------------------------------------------

def _stream_file(filepath: Path, collection_name: str | None = None) -> int:
    """
    Stream a single file through the full pipeline.

    Returns the number of chunks upserted.
    """
    logger.info("⏳ Ingesting: %s", filepath.name)
    t0 = time.perf_counter()

    # 1. Extract text
    text = extract_text(filepath)
    if not text.strip():
        logger.warning("Skipping empty file: %s", filepath.name)
        return 0

    # 2. Chunk (generator)
    chunks = chunking.chunk_text(
        text,
        source_file=str(filepath),
        extra_metadata={"ingested_at": time.time()},
    )

    # 3. Embed on-the-fly (streaming batches)
    chunk_vector_stream = embeddings.embed_chunks(chunks)

    # 4. Upsert into Qdrant (streaming batches)
    count = qdrant_utils.upsert_chunks(chunk_vector_stream, collection_name=collection_name)

    elapsed = time.perf_counter() - t0
    logger.info("✅ Ingested %s — %d chunks in %.2fs", filepath.name, count, elapsed)
    return count


def _should_ingest(filepath: Path) -> bool:
    """Check whether the file has been modified since last ingestion."""
    key = str(filepath)
    mtime = filepath.stat().st_mtime
    if key in _ingested_files and _ingested_files[key] >= mtime:
        logger.debug("Skipping unchanged file: %s", filepath.name)
        return False
    _ingested_files[key] = mtime
    return True


# ---------------------------------------------------------------------------
# Public entry-points
# ---------------------------------------------------------------------------

def ingest_file(filepath: str | Path, collection_name: str | None = None) -> int:
    """Ingest a single file into Qdrant. Returns chunk count."""
    filepath = Path(filepath)
    if not filepath.exists():
        raise FileNotFoundError(f"File not found: {filepath}")

    qdrant_utils.ensure_collection(collection_name=collection_name)
    return _stream_file(filepath, collection_name=collection_name)


def ingest_scan(
    directory: str | Path | None = None,
    collection_name: str | None = None,
) -> dict:
    """
    One-shot: scan a directory and ingest all supported files.

    Returns a summary dict with file_count, chunk_count, elapsed_seconds.
    """
    directory = Path(directory) if directory else config.CONTENT_DIR
    qdrant_utils.ensure_collection(collection_name=collection_name)

    total_files = 0
    total_chunks = 0
    t0 = time.perf_counter()

    for fe in scan_directory(directory):
        if _should_ingest(fe.path):
            count = _stream_file(fe.path, collection_name=collection_name)
            total_chunks += count
            total_files += 1

    elapsed = time.perf_counter() - t0
    summary = {
        "mode": "scan",
        "directory": str(directory),
        "files_ingested": total_files,
        "chunks_created": total_chunks,
        "elapsed_seconds": round(elapsed, 2),
    }
    logger.info("Scan complete: %s", json.dumps(summary))
    return summary


def ingest_watch(
    directory: str | Path | None = None,
    collection_name: str | None = None,
    scan_existing: bool = True,
) -> None:
    """
    Continuous mode: watch *directory* for new / modified files and
    stream each through the ingestion pipeline as it arrives.

    Blocks until interrupted (Ctrl-C / SIGINT / SIGTERM).
    """
    directory = Path(directory) if directory else config.CONTENT_DIR
    qdrant_utils.ensure_collection(collection_name=collection_name)

    watcher = DirectoryWatcher(
        directory=directory,
        scan_existing=scan_existing,
    )

    # Graceful shutdown
    def _shutdown(sig, frame):
        logger.info("Received signal %s — shutting down watcher …", sig)
        watcher.stop()

    signal.signal(signal.SIGINT, _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    logger.info("🔄  Starting continuous watcher on: %s", directory)
    logger.info("    Drop files into this directory to ingest them automatically.")
    logger.info("    Press Ctrl-C to stop.\n")

    watcher.start()

    total_files = 0
    total_chunks = 0

    try:
        for file_event in watcher:
            if _should_ingest(file_event.path):
                try:
                    count = _stream_file(file_event.path, collection_name=collection_name)
                    total_chunks += count
                    total_files += 1
                    info = qdrant_utils.collection_info(collection_name)
                    logger.info(
                        "📊 Running totals — files: %d | chunks: %d | points in DB: %s",
                        total_files,
                        total_chunks,
                        info["points_count"],
                    )
                except Exception:
                    logger.exception("Error ingesting %s — skipping", file_event.path.name)
    finally:
        watcher.stop()
        logger.info(
            "🛑 Watcher stopped. Session totals — files: %d | chunks: %d",
            total_files,
            total_chunks,
        )


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Dynamic RAG Ingestion Pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    sub = parser.add_subparsers(dest="command", required=True)

    # -- scan ---------------------------------------------------------------
    p_scan = sub.add_parser("scan", help="One-shot: scan content/ and ingest all files")
    p_scan.add_argument("--dir", type=str, default=None, help="Directory to scan")
    p_scan.add_argument("--collection", type=str, default=None)

    # -- watch --------------------------------------------------------------
    p_watch = sub.add_parser("watch", help="Continuous: watch for new files")
    p_watch.add_argument("--dir", type=str, default=None, help="Directory to watch")
    p_watch.add_argument("--collection", type=str, default=None)
    p_watch.add_argument(
        "--skip-existing", action="store_true",
        help="Don't ingest files already in the directory",
    )

    # -- file ---------------------------------------------------------------
    p_file = sub.add_parser("file", help="Ingest a single file")
    p_file.add_argument("path", type=str, help="Path to the file")
    p_file.add_argument("--collection", type=str, default=None)

    args = parser.parse_args()

    if args.command == "scan":
        summary = ingest_scan(directory=args.dir, collection_name=args.collection)
        print(json.dumps(summary, indent=2))

    elif args.command == "watch":
        ingest_watch(
            directory=args.dir,
            collection_name=args.collection,
            scan_existing=not args.skip_existing,
        )

    elif args.command == "file":
        count = ingest_file(args.path, collection_name=args.collection)
        print(f"Ingested {count} chunks from {args.path}")


if __name__ == "__main__":
    main()
