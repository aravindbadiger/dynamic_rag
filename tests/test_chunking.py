"""
Tests for the chunking module.
"""
import pytest
from chunking import Chunk, chunk_text, chunk_file
from pathlib import Path


class TestChunkText:
    def test_empty_string_yields_nothing(self):
        assert list(chunk_text("")) == []

    def test_short_text_yields_one_chunk(self):
        text = "This is a short sentence that is above the minimum size. " * 3
        chunks = list(chunk_text(text, min_chars=10, max_chars=2000))
        assert len(chunks) == 1
        assert chunks[0].chunk_index == 0
        assert chunks[0].source_file == "unknown"

    def test_long_text_splits_into_multiple_chunks(self):
        # Create text with multiple sentences that exceeds max_chars
        text = ". ".join([f"Sentence number {i} with enough words to make it real" for i in range(50)])
        text += "."
        chunks = list(chunk_text(text, max_chars=200, min_chars=20, overlap=30))
        assert len(chunks) > 1
        # Check ordering
        for i, c in enumerate(chunks):
            assert c.chunk_index == i

    def test_chunk_ids_are_deterministic(self):
        text = "Hello world this is a test sentence. Another sentence here for good measure."
        c1 = list(chunk_text(text, source_file="a.txt", min_chars=10))
        c2 = list(chunk_text(text, source_file="a.txt", min_chars=10))
        assert [c.chunk_id for c in c1] == [c.chunk_id for c in c2]

    def test_different_sources_yield_different_ids(self):
        text = "Hello world this is a test sentence. Another sentence here for good measure."
        c1 = list(chunk_text(text, source_file="a.txt", min_chars=10))
        c2 = list(chunk_text(text, source_file="b.txt", min_chars=10))
        assert c1[0].chunk_id != c2[0].chunk_id

    def test_extra_metadata_is_included(self):
        text = "A sentence with enough characters to pass the minimum threshold easily."
        chunks = list(chunk_text(text, min_chars=10, extra_metadata={"tag": "test"}))
        assert chunks[0].metadata["tag"] == "test"

    def test_char_count_in_metadata(self):
        text = "A sentence with enough characters to pass the minimum threshold easily."
        chunks = list(chunk_text(text, min_chars=10))
        for c in chunks:
            assert c.metadata["char_count"] == len(c.text)


class TestChunkFile:
    def test_file_not_found_raises(self):
        with pytest.raises(FileNotFoundError):
            list(chunk_file("/nonexistent/file.txt"))

    def test_reads_and_chunks_real_file(self, tmp_path: Path):
        f = tmp_path / "sample.txt"
        f.write_text("First sentence. Second sentence. Third sentence. " * 5)
        chunks = list(chunk_file(f, min_chars=10))
        assert len(chunks) >= 1
        assert chunks[0].source_file == str(f)


class TestChunkDataclass:
    def test_auto_generated_id(self):
        c = Chunk(text="hello world", chunk_index=0, source_file="test.txt")
        assert c.chunk_id  # non-empty
        assert "test_0_" in c.chunk_id
