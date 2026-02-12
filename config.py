
import os
# =============================================================================
# KAFKA SEARCH TOPICS
# =============================================================================
KAFKA_SEARCH_REQUEST_TOPIC = os.environ.get("KAFKA_SEARCH_REQUEST_TOPIC", "vector-search-request")
KAFKA_SEARCH_RESPONSE_TOPIC = os.environ.get("KAFKA_SEARCH_RESPONSE_TOPIC", "vector-search-response")
KAFKA_SEARCH_CONSUMER_GROUP = os.environ.get("KAFKA_SEARCH_CONSUMER_GROUP", "qdrant-search-consumer-group")
KAFKA_SEARCH_CLIENT_GROUP = os.environ.get("KAFKA_SEARCH_CLIENT_GROUP", "rag-client-search-group")
"""
Configuration settings for the Dynamic Embedding RAG project.
Supports environment variables for Docker deployment.
"""
from pathlib import Path

# =============================================================================
# ENVIRONMENT LOADING
# =============================================================================
_ENV_LOCAL_FILE = Path(__file__).parent / ".env.local"
if _ENV_LOCAL_FILE.exists():
    try:
        from dotenv import load_dotenv
        load_dotenv(_ENV_LOCAL_FILE)
        print(f"[config] Loaded environment from: {_ENV_LOCAL_FILE}")
    except ImportError:
        print("[config] Warning: python-dotenv not installed, using system environment variables")

# Project paths
PROJECT_ROOT = Path(__file__).parent
DATA_DIR = PROJECT_ROOT / "data"
CHUNKS_DIR = DATA_DIR / "chunks"
QDRANT_STORAGE_DIR = DATA_DIR / "qdrant_storage"
CONTENT_DIR = PROJECT_ROOT / "content"

# Create directories if they don't exist
DATA_DIR.mkdir(exist_ok=True)
CHUNKS_DIR.mkdir(exist_ok=True)
QDRANT_STORAGE_DIR.mkdir(exist_ok=True)
CONTENT_DIR.mkdir(exist_ok=True)

# =============================================================================
# EMBEDDING SETTINGS
# =============================================================================
EMBEDDING_MODEL = os.environ.get("EMBEDDING_MODEL", "all-MiniLM-L6-v2")

EMBEDDING_MODELS = {
    "all-MiniLM-L6-v2": 384,
    "all-mpnet-base-v2": 768,
    "BAAI/bge-large-en-v1.5": 1024,
    "BAAI/bge-small-en-v1.5": 384,
}

EMBEDDING_DIM = EMBEDDING_MODELS.get(EMBEDDING_MODEL, 384)

# =============================================================================
# QDRANT SETTINGS
# =============================================================================
QDRANT_HOST = os.environ.get("QDRANT_HOST", "localhost")
QDRANT_PORT = int(os.environ.get("QDRANT_PORT", "6333"))
QDRANT_COLLECTION = os.environ.get("QDRANT_COLLECTION", "dynamic-rag")
USE_QDRANT_SERVER = os.environ.get("USE_QDRANT_SERVER", "true").lower() == "true"

# =============================================================================
# CHUNKING SETTINGS
# =============================================================================
CHUNK_MAX_CHARS = int(os.environ.get("CHUNK_MAX_CHARS", "512"))
CHUNK_MIN_CHARS = int(os.environ.get("CHUNK_MIN_CHARS", "64"))
CHUNK_OVERLAP = int(os.environ.get("CHUNK_OVERLAP", "50"))

# =============================================================================
# RAG SETTINGS
# =============================================================================
SEARCH_THRESHOLD = float(os.environ.get("SEARCH_THRESHOLD", "0.5"))
TOP_K_RESULTS = int(os.environ.get("TOP_K_RESULTS", "5"))

# =============================================================================
# LLM SETTINGS
# =============================================================================
LLM_PROVIDER = os.environ.get("LLM_PROVIDER", "ollama")
OLLAMA_MODEL = os.environ.get("OLLAMA_MODEL", "llama3.2")
OLLAMA_BASE_URL = os.environ.get("OLLAMA_BASE_URL", "http://localhost:11434")

OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY", "")
OPENAI_BASE_URL = os.environ.get("OPENAI_BASE_URL", "https://api.openai.com/v1")
OPENAI_MODEL = os.environ.get("OPENAI_MODEL", "gpt-3.5-turbo")

GITHUB_TOKEN = os.environ.get("GITHUB_TOKEN", "")
GHCP_MODEL = os.environ.get("GHCP_MODEL", "gpt-4o")
GHCP_BASE_URL = os.environ.get("GHCP_BASE_URL", "https://models.inference.ai.azure.com")

# =============================================================================
# GRADIO UI SETTINGS
# =============================================================================
GRADIO_SERVER_NAME = os.environ.get("GRADIO_SERVER_NAME", "0.0.0.0")
GRADIO_SERVER_PORT = int(os.environ.get("GRADIO_SERVER_PORT", "7861"))
GRADIO_SHARE = os.environ.get("GRADIO_SHARE", "false").lower() == "true"
