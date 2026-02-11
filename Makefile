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
