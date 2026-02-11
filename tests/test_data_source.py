"""
Tests for the data_source module.
"""
import time
from pathlib import Path

import pytest
from data_source import (
    DirectoryWatcher,
    FileEvent,
    extract_text,
    scan_directory,
    SUPPORTED_EXTENSIONS,
)


class TestFileEvent:
    def test_extension(self):
        fe = FileEvent(path=Path("test.txt"))
        assert fe.extension == ".txt"

    def test_repr(self):
        fe = FileEvent(path=Path("test.md"), event_type="created")
        assert "created" in repr(fe) and "test.md" in repr(fe)


class TestExtractText:
    def test_plain_text(self, tmp_path: Path):
        f = tmp_path / "sample.txt"
        f.write_text("Hello, world!")
        assert extract_text(f) == "Hello, world!"

    def test_json_file(self, tmp_path: Path):
        f = tmp_path / "data.json"
        f.write_text('{"key": "value"}')
        result = extract_text(f)
        assert "key" in result and "value" in result

    def test_markdown_file(self, tmp_path: Path):
        f = tmp_path / "readme.md"
        f.write_text("# Title\n\nSome content here.")
        result = extract_text(f)
        assert "Title" in result


class TestScanDirectory:
    def test_empty_dir_yields_nothing(self, tmp_path: Path):
        events = list(scan_directory(tmp_path))
        assert events == []

    def test_finds_supported_files(self, tmp_path: Path):
        (tmp_path / "a.txt").write_text("hello")
        (tmp_path / "b.md").write_text("world")
        (tmp_path / "c.bin").write_text("skip me")  # unsupported
        events = list(scan_directory(tmp_path))
        names = {e.path.name for e in events}
        assert "a.txt" in names
        assert "b.md" in names
        assert "c.bin" not in names

    def test_recursive_scan(self, tmp_path: Path):
        sub = tmp_path / "subdir"
        sub.mkdir()
        (sub / "nested.txt").write_text("nested content")
        events = list(scan_directory(tmp_path))
        assert any("nested.txt" in str(e.path) for e in events)


class TestDirectoryWatcher:
    def test_scan_existing_files(self, tmp_path: Path):
        (tmp_path / "existing.txt").write_text("I was here first")

        watcher = DirectoryWatcher(directory=tmp_path, scan_existing=True)
        watcher.start()
        events = watcher.drain()
        watcher.stop()

        assert len(events) == 1
        assert events[0].path.name == "existing.txt"

    def test_detect_new_file(self, tmp_path: Path):
        watcher = DirectoryWatcher(directory=tmp_path, scan_existing=False)
        watcher.start()

        # Give the observer a moment to initialize
        time.sleep(0.5)

        # Create a new file
        (tmp_path / "new_file.txt").write_text("Dynamic content!")
        time.sleep(1.5)  # Allow watchdog to detect

        events = watcher.drain()
        watcher.stop()

        assert any(e.path.name == "new_file.txt" for e in events)

    def test_context_manager(self, tmp_path: Path):
        (tmp_path / "ctx.txt").write_text("context manager test")
        with DirectoryWatcher(directory=tmp_path) as w:
            assert w.is_running
            events = w.drain()
            assert len(events) >= 1
        assert not w.is_running
