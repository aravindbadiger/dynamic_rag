"""
Tests for the embeddings module.
"""
import numpy as np
import pytest

import embeddings


class TestEmbedText:
    def test_returns_numpy_array(self):
        vec = embeddings.embed_text("hello world")
        assert isinstance(vec, np.ndarray)

    def test_correct_dimension(self):
        vec = embeddings.embed_text("test sentence")
        assert vec.shape == (embeddings.get_dimension(),)

    def test_different_texts_different_vectors(self):
        v1 = embeddings.embed_text("The cat sat on the mat.")
        v2 = embeddings.embed_text("Quantum computing leverages superposition.")
        # They should not be identical
        assert not np.allclose(v1, v2, atol=1e-3)


class TestEmbedTexts:
    def test_batch_shape(self):
        texts = ["one", "two", "three"]
        vecs = embeddings.embed_texts(texts)
        assert vecs.shape == (3, embeddings.get_dimension())

    def test_consistent_with_single(self):
        text = "consistency check"
        single = embeddings.embed_text(text)
        batch = embeddings.embed_texts([text])
        np.testing.assert_allclose(single, batch[0], atol=1e-5)


class TestEmbedChunks:
    def test_streaming_yield(self):
        from chunking import Chunk

        chunks = [
            Chunk(text="First chunk of text.", chunk_index=0, source_file="test.txt"),
            Chunk(text="Second chunk of text.", chunk_index=1, source_file="test.txt"),
        ]
        results = list(embeddings.embed_chunks(iter(chunks)))
        assert len(results) == 2
        for chunk, vec in results:
            assert isinstance(vec, np.ndarray)
            assert vec.shape == (embeddings.get_dimension(),)
