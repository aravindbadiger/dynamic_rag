"""
LLM module — pluggable provider abstraction.

Supports three backends, selected via ``LLM_PROVIDER`` env var:
  • ``ollama``  — local Ollama server
  • ``openai``  — OpenAI-compatible API
  • ``ghcp``    — GitHub Copilot / Azure Models Inference API

All providers expose the same interface through ``get_provider()`` which
returns an ``LLMProvider`` instance with ``.chat()`` and ``.stream()``
methods.

Usage::

    from llm import get_provider

    llm = get_provider()                     # uses config.LLM_PROVIDER
    answer = llm.chat("Explain RAG.")        # blocking
    for chunk in llm.stream("Explain RAG."): # streaming
        print(chunk, end="")
"""
from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from typing import Generator, List, Optional

import config

logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════════════════
# Base class
# ═══════════════════════════════════════════════════════════════════════════

class LLMProvider(ABC):
    """Abstract base class for LLM providers."""

    @property
    @abstractmethod
    def name(self) -> str:
        """Human-readable provider name."""
        ...

    @property
    @abstractmethod
    def model(self) -> str:
        """Model identifier in use."""
        ...

    @abstractmethod
    def chat(
        self,
        user_message: str,
        system_prompt: str | None = None,
        temperature: float = 0.7,
        max_tokens: int = 1024,
    ) -> str:
        """Send a message and return the full response text."""
        ...

    @abstractmethod
    def stream(
        self,
        user_message: str,
        system_prompt: str | None = None,
        temperature: float = 0.7,
        max_tokens: int = 1024,
    ) -> Generator[str, None, None]:
        """Stream response tokens as they arrive."""
        ...

    def rag_chat(
        self,
        question: str,
        context_chunks: List[str],
        system_prompt: str | None = None,
        temperature: float = 0.3,
        max_tokens: int = 1024,
    ) -> str:
        """
        RAG-specific chat: build a prompt from retrieved context chunks
        and the user's question, then return the LLM answer.
        """
        if system_prompt is None:
            system_prompt = (
                "You are a helpful assistant. Answer the user's question using ONLY "
                "the provided context. If the context does not contain enough information, "
                "say so. Do not make up information."
            )

        context_block = "\n\n---\n\n".join(context_chunks)
        user_message = (
            f"Context:\n{context_block}\n\n"
            f"Question: {question}\n\n"
            f"Answer:"
        )
        return self.chat(
            user_message,
            system_prompt=system_prompt,
            temperature=temperature,
            max_tokens=max_tokens,
        )

    def rag_stream(
        self,
        question: str,
        context_chunks: List[str],
        system_prompt: str | None = None,
        temperature: float = 0.3,
        max_tokens: int = 1024,
    ) -> Generator[str, None, None]:
        """
        RAG-specific streaming chat: same prompt construction as
        ``rag_chat`` but yields tokens.
        """
        if system_prompt is None:
            system_prompt = (
                "You are a helpful assistant. Answer the user's question using ONLY "
                "the provided context. If the context does not contain enough information, "
                "say so. Do not make up information."
            )

        context_block = "\n\n---\n\n".join(context_chunks)
        user_message = (
            f"Context:\n{context_block}\n\n"
            f"Question: {question}\n\n"
            f"Answer:"
        )
        yield from self.stream(
            user_message,
            system_prompt=system_prompt,
            temperature=temperature,
            max_tokens=max_tokens,
        )

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(model={self.model!r})"


# ═══════════════════════════════════════════════════════════════════════════
# Ollama provider
# ═══════════════════════════════════════════════════════════════════════════

class OllamaProvider(LLMProvider):
    """LLM provider using a local Ollama server."""

    def __init__(
        self,
        model: str | None = None,
        base_url: str | None = None,
    ):
        self._model = model or config.OLLAMA_MODEL
        self._base_url = base_url or config.OLLAMA_BASE_URL
        self._client = None

    @property
    def name(self) -> str:
        return "Ollama"

    @property
    def model(self) -> str:
        return self._model

    def _get_client(self):
        if self._client is None:
            import ollama as _ollama
            self._client = _ollama.Client(host=self._base_url)
            logger.info("Ollama client connected to %s", self._base_url)
        return self._client

    def chat(
        self,
        user_message: str,
        system_prompt: str | None = None,
        temperature: float = 0.7,
        max_tokens: int = 1024,
    ) -> str:
        messages = []
        if system_prompt:
            messages.append({"role": "system", "content": system_prompt})
        messages.append({"role": "user", "content": user_message})

        client = self._get_client()
        response = client.chat(
            model=self._model,
            messages=messages,
            options={"temperature": temperature, "num_predict": max_tokens},
        )
        return response["message"]["content"]

    def stream(
        self,
        user_message: str,
        system_prompt: str | None = None,
        temperature: float = 0.7,
        max_tokens: int = 1024,
    ) -> Generator[str, None, None]:
        messages = []
        if system_prompt:
            messages.append({"role": "system", "content": system_prompt})
        messages.append({"role": "user", "content": user_message})

        client = self._get_client()
        stream = client.chat(
            model=self._model,
            messages=messages,
            options={"temperature": temperature, "num_predict": max_tokens},
            stream=True,
        )
        for chunk in stream:
            token = chunk["message"]["content"]
            if token:
                yield token


# ═══════════════════════════════════════════════════════════════════════════
# OpenAI provider
# ═══════════════════════════════════════════════════════════════════════════

class OpenAIProvider(LLMProvider):
    """LLM provider using the OpenAI API (or any compatible endpoint)."""

    def __init__(
        self,
        model: str | None = None,
        api_key: str | None = None,
        base_url: str | None = None,
    ):
        self._model = model or config.OPENAI_MODEL
        self._api_key = api_key or config.OPENAI_API_KEY
        self._base_url = base_url or config.OPENAI_BASE_URL
        self._client = None

    @property
    def name(self) -> str:
        return "OpenAI"

    @property
    def model(self) -> str:
        return self._model

    def _get_client(self):
        if self._client is None:
            from openai import OpenAI
            self._client = OpenAI(
                api_key=self._api_key,
                base_url=self._base_url,
            )
            logger.info("OpenAI client initialised (base_url=%s)", self._base_url)
        return self._client

    def chat(
        self,
        user_message: str,
        system_prompt: str | None = None,
        temperature: float = 0.7,
        max_tokens: int = 1024,
    ) -> str:
        messages = []
        if system_prompt:
            messages.append({"role": "system", "content": system_prompt})
        messages.append({"role": "user", "content": user_message})

        client = self._get_client()
        response = client.chat.completions.create(
            model=self._model,
            messages=messages,
            temperature=temperature,
            max_tokens=max_tokens,
        )
        return response.choices[0].message.content or ""

    def stream(
        self,
        user_message: str,
        system_prompt: str | None = None,
        temperature: float = 0.7,
        max_tokens: int = 1024,
    ) -> Generator[str, None, None]:
        messages = []
        if system_prompt:
            messages.append({"role": "system", "content": system_prompt})
        messages.append({"role": "user", "content": user_message})

        client = self._get_client()
        stream = client.chat.completions.create(
            model=self._model,
            messages=messages,
            temperature=temperature,
            max_tokens=max_tokens,
            stream=True,
        )
        for chunk in stream:
            delta = chunk.choices[0].delta
            if delta and delta.content:
                yield delta.content


# ═══════════════════════════════════════════════════════════════════════════
# GitHub Copilot (GHCP) provider
# ═══════════════════════════════════════════════════════════════════════════

class GHCPProvider(LLMProvider):
    """
    LLM provider using GitHub Copilot / Azure Models Inference API.

    Uses the OpenAI client under the hood, pointed at the GHCP endpoint
    with a GitHub token for authentication.
    """

    def __init__(
        self,
        model: str | None = None,
        token: str | None = None,
        base_url: str | None = None,
    ):
        self._model = model or config.GHCP_MODEL
        self._token = token or config.GITHUB_TOKEN
        self._base_url = base_url or config.GHCP_BASE_URL
        self._client = None

    @property
    def name(self) -> str:
        return "GitHub Copilot"

    @property
    def model(self) -> str:
        return self._model

    def _get_client(self):
        if self._client is None:
            from openai import OpenAI
            self._client = OpenAI(
                api_key=self._token,
                base_url=self._base_url,
            )
            logger.info("GHCP client initialised (base_url=%s)", self._base_url)
        return self._client

    def chat(
        self,
        user_message: str,
        system_prompt: str | None = None,
        temperature: float = 0.7,
        max_tokens: int = 1024,
    ) -> str:
        messages = []
        if system_prompt:
            messages.append({"role": "system", "content": system_prompt})
        messages.append({"role": "user", "content": user_message})

        client = self._get_client()
        response = client.chat.completions.create(
            model=self._model,
            messages=messages,
            temperature=temperature,
            max_tokens=max_tokens,
        )
        return response.choices[0].message.content or ""

    def stream(
        self,
        user_message: str,
        system_prompt: str | None = None,
        temperature: float = 0.7,
        max_tokens: int = 1024,
    ) -> Generator[str, None, None]:
        messages = []
        if system_prompt:
            messages.append({"role": "system", "content": system_prompt})
        messages.append({"role": "user", "content": user_message})

        client = self._get_client()
        stream = client.chat.completions.create(
            model=self._model,
            messages=messages,
            temperature=temperature,
            max_tokens=max_tokens,
            stream=True,
        )
        for chunk in stream:
            delta = chunk.choices[0].delta
            if delta and delta.content:
                yield delta.content


# ═══════════════════════════════════════════════════════════════════════════
# Provider registry & factory
# ═══════════════════════════════════════════════════════════════════════════

_PROVIDERS = {
    "ollama": OllamaProvider,
    "openai": OpenAIProvider,
    "ghcp": GHCPProvider,
}

_cached_provider: LLMProvider | None = None


def get_provider(provider_name: str | None = None, **kwargs) -> LLMProvider:
    """
    Return an LLMProvider instance for the given provider name.

    Uses ``config.LLM_PROVIDER`` by default. The instance is cached
    (singleton) when called without explicit arguments.
    """
    global _cached_provider
    name = (provider_name or config.LLM_PROVIDER).lower().strip()

    # Return cached instance if no overrides
    if not provider_name and not kwargs and _cached_provider is not None:
        return _cached_provider

    cls = _PROVIDERS.get(name)
    if cls is None:
        available = ", ".join(sorted(_PROVIDERS.keys()))
        raise ValueError(
            f"Unknown LLM provider: {name!r}. Available: {available}"
        )

    instance = cls(**kwargs)
    logger.info("LLM provider: %s (model=%s)", instance.name, instance.model)

    if not provider_name and not kwargs:
        _cached_provider = instance

    return instance


def list_providers() -> List[str]:
    """Return the list of available provider names."""
    return sorted(_PROVIDERS.keys())
