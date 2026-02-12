# ---------- Docker: Status Service ----------
docker-status-service-up:
	docker compose up -d status-service

docker-status-service-down:
	docker compose stop status-service

docker-status-service-logs:
	docker compose logs -f status-service

docker-status-service-restart:
	docker compose restart status-service
# ---------- Docker: Scaling Consumers ----------
docker-fs-consumer-scale:
	docker compose up -d --scale file-system-consumer=$$N
	@echo "Scaled file-system-consumer to $$N instances. Usage: make docker-fs-consumer-scale N=3"

docker-mongo-consumer-scale:
	docker compose up -d --scale mongo-db-consumer=$$N
	@echo "Scaled mongo-db-consumer to $$N instances. Usage: make docker-mongo-consumer-scale N=2"

# ---------- Docker: Logs & Status ----------
docker-fs-consumer-logs:
	docker compose logs -f file-system-consumer

docker-mongo-consumer-logs:
	docker compose logs -f mongo-db-consumer

docker-fs-producer-logs:
	docker compose logs -f file-system-producer

docker-mongo-producer-logs:
	docker compose logs -f mongo-db-producer

docker-rag-app-logs:
	docker compose logs -f rag-app

docker-status:
	docker compose ps

# ---------- Docker: Restart Individual Services ----------
docker-fs-consumer-restart:
	docker compose restart file-system-consumer

docker-mongo-consumer-restart:
	docker compose restart mongo-db-consumer

docker-fs-producer-restart:
	docker compose restart file-system-producer

docker-mongo-producer-restart:
	docker compose restart mongo-db-producer

docker-rag-app-restart:
	docker compose restart rag-app
# ---------- Docker: File System Producer ----------
docker-fs-producer-up:
	docker compose up -d file-system-producer

docker-fs-producer-down:
	docker compose stop file-system-producer

# ---------- Docker: MongoDB Producer ----------
docker-mongo-producer-up:
	docker compose up -d mongo-db-producer

docker-mongo-producer-down:
	docker compose stop mongo-db-producer

# ---------- Docker: File System Consumer ----------
docker-fs-consumer-up:
	docker compose up -d file-system-consumer

docker-fs-consumer-down:
	docker compose stop file-system-consumer

# ---------- Docker: MongoDB Consumer ----------
docker-mongo-consumer-up:
	docker compose up -d mongo-db-consumer

docker-mongo-consumer-down:
	docker compose stop mongo-db-consumer

# ---------- Docker: Kafka & Zookeeper ----------
docker-kafka-up:
	docker compose up -d zookeeper kafka

docker-kafka-down:
	docker compose stop kafka zookeeper

docker-kafka-logs:
	docker compose logs -f kafka

# ---------- Docker: Core Services Only (no Kafka) ----------
docker-core-up:
	docker compose up -d qdrant ollama rag-app

docker-core-down:
	docker compose stop qdrant ollama rag-app

# ---------- Docker: All Services ----------
docker-all-up:
	docker compose up -d

docker-all-down:
	docker compose down

.PHONY: install run test clean ingest-scan ingest-watch schema-create schema-info \
       docker-build docker-up docker-down docker-logs docker-restart \
       docker-ingest-scan docker-ingest-watch docker-schema-create docker-schema-info

# ==========================================================================
# Local development
# ==========================================================================

install:
	pip install -r requirements.txt

run:
	python rag_client.py

test:
	python -m pytest tests/ -v

clean:
	find . -type d -name __pycache__ -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete

# ---------- Schema management ----------
schema-create:
	python schema_manager.py create

schema-info:
	python schema_manager.py info

schema-delete:
	python schema_manager.py delete

# ---------- Ingestion pipeline ----------
ingest-scan:
	python ingest_data.py scan

ingest-watch:
	python ingest_data.py watch

ingest-file:
	@echo "Usage: make ingest-file FILE=path/to/file.txt"
	python ingest_data.py file $(FILE)

# ==========================================================================
# Docker
# ==========================================================================

docker-build:
	docker compose build

docker-up:
	docker compose up -d

docker-up-build:
	docker compose up -d --build

docker-down:
	docker compose down

docker-down-volumes:
	docker compose down -v

docker-logs:
	docker compose logs -f rag-app

docker-logs-ollama:
	docker compose logs -f ollama

docker-restart:
	docker compose restart rag-app

# ---------- Docker: Ollama ----------
docker-ollama-pull:
	@echo "Pulling model $(or $(MODEL),llama3.2) into Ollama container…"
	docker compose exec ollama ollama pull $(or $(MODEL),llama3.2)

docker-ollama-list:
	docker compose exec ollama ollama list

# ---------- Docker: schema management ----------
docker-schema-create:
	docker compose exec rag-app python schema_manager.py create

docker-schema-info:
	docker compose exec rag-app python schema_manager.py info

docker-schema-delete:
	docker compose exec rag-app python schema_manager.py delete

# ---------- Docker: ingestion pipeline ----------
docker-ingest-scan:
	docker compose exec rag-app python ingest_data.py scan

docker-ingest-watch:
	docker compose exec rag-app python ingest_data.py watch

docker-ingest-file:
	@echo "Usage: make docker-ingest-file FILE=content/yourfile.txt"
	docker compose exec rag-app python ingest_data.py file $(FILE)

# ---------- Docker: shell ----------
docker-shell:
	docker compose exec rag-app bash
