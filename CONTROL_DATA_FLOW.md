# Control and Data Flow Documentation

## 1. Overview
This document describes the control and data flow in the Dynamic Embedding RAG Project, covering ingestion, chunking, embedding, vector storage, search, and LLM-powered answer generation. It also details how Kafka orchestrates communication between microservices.

---

## 2. Control Flow

### Ingestion Pipeline
- **File System Producer**: Watches the `content/` directory for new/modified files. On event:
  - Reads file content
  - Chunks content to fit Kafka message size
  - Sends each chunk as a Kafka message (with metadata)
  - Publishes status updates to Kafka
- **MongoDB Producer**: Watches MongoDB for new documents. On event:
  - Reads document content
  - Chunks content to fit Kafka message size
  - Sends each chunk as a Kafka message (with metadata)
  - Publishes status updates to Kafka

### Consumers
- **File System Consumer**: Listens for chunked file messages from Kafka. For each chunk:
  - Embeds chunk
  - Upserts embedding into Qdrant Vector DB
- **MongoDB Consumer**: Listens for chunked document messages from Kafka. For each chunk:
  - Embeds chunk
  - Upserts embedding into Qdrant Vector DB

### Status Service
- Aggregates status and stats from Kafka
- Exposes HTTP API for UI and monitoring

### Qdrant Stats Producer
- Periodically queries Qdrant for stats
- Publishes stats to Kafka

### RAG Client (UI)
- Submits queries
- Fetches status and stats from Status Service
- Sends queries to LLM for answer generation

---

## 3. Data Flow

### Ingestion
1. File/document event triggers producer
2. Content is chunked and sent to Kafka
3. Consumer receives chunked messages
4. Each chunk is embedded and upserted into Qdrant

### Query
1. User submits query via UI
2. Query is embedded
3. Qdrant returns relevant chunks
4. Chunks are sent to LLM for answer generation
5. Answer is returned to UI

### Status & Stats
- Producers publish status to Kafka
- Status Service aggregates and exposes via API
- Qdrant Stats Producer publishes stats to Kafka
- UI fetches status and stats from Status Service

---

## 4. Sequence Diagrams

### Ingestion Sequence
```
[Producer] --(chunked content)--> [Kafka] --(chunked content)--> [Consumer] --(embed/upsert)--> [Qdrant]
[Producer] --(status)-----------> [Kafka] --(status)-----------> [Status Service]
```

### Query Sequence
```
[RAG Client] --(query)--> [Embedding Model] --(vector search)--> [Qdrant]
     ^                                              |
     |                                              v
[Status Service] <-------------------------- [Qdrant Stats Producer]
     ^                                              |
     |                                              v
[RAG Client] <------------------------------ [Status Service]
     |
     v
[RAG Client] --(context)--> [LLM] --(answer)--> [RAG Client]
```

---

## 5. References
- ARCHITECTURE_NEW.md
- README.md
- docker-compose.yml
- file_system_producer.py
- mongo_db_producer.py
- file_system_consumer.py
- mongo_db_consumer.py
- status_service.py
- rag_client.py
- qdrant_utils.py
- llm.py

---

*For further details, see referenced files and code comments.*
