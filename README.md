# Dynamic Embedding RAG Project

A Retrieval-Augmented Generation project with **dynamic, streaming ingestion** into the Qdrant vector database. Drop files into the `content/` directory and they are automatically chunked, embedded, and ingested into Qdrant in real time.

## Architecture

```
content/  (data source — drop files here)
   │
   ▼
DirectoryWatcher (watchdog)
   │  detects new / modified files
   ▼
extract_text()          ← data_source.py
   │
   ▼
chunk_text()            ← chunking.py       (sentence-aware, configurable overlap)
   │  generator / stream
   ▼
embed_chunks()          ← embeddings.py     (sentence-transformers, batched)
   │  generator / stream
   ▼
upsert_chunks()         ← qdrant_utils.py   (batched upsert into Qdrant)
   │
   ▼
Qdrant Vector DB
```

The entire pipeline is **streaming** — chunks flow through generators so memory usage stays constant regardless of file size.

## Project Structure

```
dynamic_embedding_rag_project/
├── config.py            # Configuration & environment settings
├── data_source.py       # Dynamic data source & file watcher
├── chunking.py          # Sentence-aware document chunking
├── embeddings.py        # Embedding model (sentence-transformers)
├── qdrant_utils.py      # Qdrant vector DB operations
├── ingest_data.py       # Streaming ingestion pipeline (scan / watch / file)
├── schema_manager.py    # Collection management CLI
├── rag_client.py        # RAG query client
├── llm.py               # LLM provider abstraction
├── requirements.txt     # Python dependencies
├── Makefile             # Common commands
├── content/             # ← Drop source documents here
├── data/
│   ├── chunks/          # Processed text chunks
│   └── qdrant_storage/  # Local Qdrant storage (if not using server)
├── diagrams/            # Architecture diagrams
└── tests/               # Test suite
```

## Quick Start

### 1. Clone and Setup

```bash
git clone <repo-url>
cd dynamic_embedding_rag_project
cp .env.example .env.local  # (optional) customize environment
```

### 2. Install Dependencies (Local Python)

```bash
make install
```

### 3. Start All Services (Recommended: Docker Compose)

```bash
docker-compose up --build
```
- This launches Kafka, Qdrant, producers, consumers, status service, and UI in containers.
- Logs for each service are visible in the terminal.

### 4. Initialize Qdrant Collection (if running locally)

```bash
make schema-create
```

### 5. Ingest Data
- **Drop files** into the `content/` directory. They will be detected and ingested automatically.
- Or trigger ingestion manually:

```bash
make ingest-scan      # One-shot: ingest all files in content/
make ingest-watch     # Continuous: watch for new files
```

### 6. Query the Vector DB

```bash
python rag_client.py "What is machine learning?"
python rag_client.py --interactive
```

### 7. Monitor Status & Troubleshooting
- Visit the Status Service API (see logs for port)
- Check logs for errors (Kafka message size, embedding, etc.)
- If you see `MSG_SIZE_TOO_LARGE`, ensure chunking is enabled and Kafka is using default or increased message size.

---

For advanced usage, ingestion modes, and schema management, see below.

## Ingestion Modes

### One-shot scan
```bash
python ingest_data.py scan                    # ingest everything in content/
python ingest_data.py scan --dir /other/path  # custom directory
```

### Continuous watch (streaming)
```bash
python ingest_data.py watch                   # watch content/ for new files
python ingest_data.py watch --skip-existing   # only process newly added files
```

### Single file
```bash
python ingest_data.py file content/paper.txt
```

## Schema Management

```bash
python schema_manager.py create              # create collection
python schema_manager.py create --recreate   # drop and re-create
python schema_manager.py info                # show collection stats
python schema_manager.py delete              # delete collection
python schema_manager.py list                # list all collections
```

## Configuration

All settings can be overridden via environment variables or `.env.local`:

| Variable | Default | Description |
|---|---|---|
| `EMBEDDING_MODEL` | `all-MiniLM-L6-v2` | Sentence-transformer model |
| `QDRANT_HOST` | `localhost` | Qdrant server host |
| `QDRANT_PORT` | `6333` | Qdrant server port |
| `QDRANT_COLLECTION` | `dynamic-rag` | Collection name |
| `USE_QDRANT_SERVER` | `true` | Use server (false = local disk) |
| `CHUNK_MAX_CHARS` | `512` | Max chunk size in characters |
| `CHUNK_OVERLAP` | `50` | Overlap between chunks |
| `TOP_K_RESULTS` | `5` | Default search result count |
| `SEARCH_THRESHOLD` | `0.5` | Minimum similarity score |

## Supported File Types

`.txt`, `.md`, `.html`, `.htm`, `.json`, `.csv`

## Testing

```bash
make test
```
