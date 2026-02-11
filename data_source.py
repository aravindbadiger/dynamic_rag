"""
Dynamic Data Source — watches a directory for new or modified files and
emits file-paths into a processing queue.

Supports two modes:
  1. **Scan mode** — one-shot scan of the content directory (existing files).
  2. **Watch mode** — continuously watches for new / modified files using
     the ``watchdog`` library and pushes them into an asyncio-compatible
     queue for downstream streaming.

Supported file types: .txt, .md, .html, .json, .csv, .pdf (text extraction only)
"""
from __future__ import annotations

import json
import logging
import os
import queue
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Generator, List, Optional, Set

from watchdog.events import FileSystemEventHandler, FileSystemEvent
from watchdog.observers import Observer

import config

logger = logging.getLogger(__name__)

# File types we know how to ingest
SUPPORTED_EXTENSIONS: Set[str] = {".txt", ".md", ".html", ".htm", ".json", ".csv"}


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------
@dataclass
class FileEvent:
    """Represents a new or modified file that should be ingested."""
    path: Path
    event_type: str = "created"  # created | modified
    timestamp: float = field(default_factory=time.time)

    @property
    def extension(self) -> str:
        return self.path.suffix.lower()

    def __repr__(self) -> str:
        return f"FileEvent({self.event_type}, {self.path.name})"


# ---------------------------------------------------------------------------
# Text extractors per file type
# ---------------------------------------------------------------------------

def _read_plain(path: Path) -> str:
    return path.read_text(encoding="utf-8", errors="replace")


def _read_html(path: Path) -> str:
    try:
        from bs4 import BeautifulSoup
        raw = path.read_text(encoding="utf-8", errors="replace")
        soup = BeautifulSoup(raw, "lxml")
        # Remove script/style tags
        for tag in soup(["script", "style"]):
            tag.decompose()
        return soup.get_text(separator="\n", strip=True)
    except ImportError:
        logger.warning("beautifulsoup4/lxml not installed — reading HTML as raw text")
        return _read_plain(path)


def _read_json(path: Path) -> str:
    """Flatten a JSON file into readable text."""
    raw = path.read_text(encoding="utf-8", errors="replace")
    try:
        data = json.loads(raw)
        return json.dumps(data, indent=2, ensure_ascii=False)
    except json.JSONDecodeError:
        return raw


_EXTRACTORS = {
    ".txt": _read_plain,
    ".md": _read_plain,
    ".csv": _read_plain,
    ".html": _read_html,
    ".htm": _read_html,
    ".json": _read_json,
}


def extract_text(path: Path) -> str:
    """Extract text content from a supported file."""
    ext = path.suffix.lower()
    extractor = _EXTRACTORS.get(ext, _read_plain)
    return extractor(path)


# ---------------------------------------------------------------------------
# Directory scanner (one-shot)
# ---------------------------------------------------------------------------

def scan_directory(
    directory: Path | str | None = None,
    extensions: Set[str] | None = None,
) -> Generator[FileEvent, None, None]:
    """Yield FileEvent for every supported file in *directory* (recursive)."""
    directory = Path(directory) if directory else config.CONTENT_DIR
    extensions = extensions or SUPPORTED_EXTENSIONS

    if not directory.exists():
        logger.warning("Content directory does not exist: %s", directory)
        return

    for root, _dirs, files in os.walk(directory):
        for fname in sorted(files):
            fpath = Path(root) / fname
            if fpath.suffix.lower() in extensions:
                yield FileEvent(path=fpath, event_type="created")


# ---------------------------------------------------------------------------
# File-system watcher (continuous)
# ---------------------------------------------------------------------------

class _IngestHandler(FileSystemEventHandler):
    """Watchdog handler that pushes FileEvents into a thread-safe queue."""

    def __init__(
        self,
        event_queue: queue.Queue[FileEvent],
        extensions: Set[str],
    ) -> None:
        super().__init__()
        self._queue = event_queue
        self._extensions = extensions

    def _accept(self, path: str) -> bool:
        return Path(path).suffix.lower() in self._extensions

    def on_created(self, event: FileSystemEvent) -> None:
        if not event.is_directory and self._accept(event.src_path):
            fe = FileEvent(path=Path(event.src_path), event_type="created")
            logger.info("New file detected: %s", fe.path.name)
            self._queue.put(fe)

    def on_modified(self, event: FileSystemEvent) -> None:
        if not event.is_directory and self._accept(event.src_path):
            fe = FileEvent(path=Path(event.src_path), event_type="modified")
            logger.debug("Modified file detected: %s", fe.path.name)
            self._queue.put(fe)


class DirectoryWatcher:
    """
    Watches the content directory for new / modified files and makes them
    available via an iterator interface.

    Usage::

        watcher = DirectoryWatcher()
        watcher.start()
        for file_event in watcher:   # blocks until a new file arrives
            process(file_event)
        watcher.stop()
    """

    def __init__(
        self,
        directory: Path | str | None = None,
        extensions: Set[str] | None = None,
        scan_existing: bool = True,
    ) -> None:
        self.directory = Path(directory) if directory else config.CONTENT_DIR
        self.extensions = extensions or SUPPORTED_EXTENSIONS
        self.scan_existing = scan_existing

        self._queue: queue.Queue[FileEvent] = queue.Queue()
        self._observer: Optional[Observer] = None
        self._running = False
        self._seen_files: Set[str] = set()

    # -- lifecycle ----------------------------------------------------------

    def start(self) -> None:
        """Start watching. Optionally scans existing files first."""
        self.directory.mkdir(parents=True, exist_ok=True)

        if self.scan_existing:
            for fe in scan_directory(self.directory, self.extensions):
                self._queue.put(fe)
                self._seen_files.add(str(fe.path))

        handler = _IngestHandler(self._queue, self.extensions)
        self._observer = Observer()
        self._observer.schedule(handler, str(self.directory), recursive=True)
        self._observer.daemon = True
        self._observer.start()
        self._running = True
        logger.info(
            "Watching directory: %s (existing files queued: %d)",
            self.directory,
            self._queue.qsize(),
        )

    def stop(self) -> None:
        """Stop the watcher."""
        self._running = False
        if self._observer:
            self._observer.stop()
            self._observer.join(timeout=5)
            self._observer = None
        logger.info("Directory watcher stopped.")

    # -- iterator interface -------------------------------------------------

    def __iter__(self):
        return self

    def __next__(self) -> FileEvent:
        """Block until a FileEvent is available (or watcher is stopped)."""
        while self._running:
            try:
                event = self._queue.get(timeout=1.0)
                return event
            except queue.Empty:
                continue
        raise StopIteration

    def drain(self, timeout: float = 0.1) -> List[FileEvent]:
        """Non-blocking: drain all currently queued events."""
        events: List[FileEvent] = []
        while True:
            try:
                events.append(self._queue.get_nowait())
            except queue.Empty:
                break
        return events

    @property
    def is_running(self) -> bool:
        return self._running

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, *exc):
        self.stop()
