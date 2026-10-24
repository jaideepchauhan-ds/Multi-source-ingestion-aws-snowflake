.PHONY: install test lint dry-run docker-build docker-dry-run

install:
	pip install -r requirements-dev.txt

test:
	pytest -q

lint:
	ruff check .

dry-run:
	python -m ingestion.main --dry-run

docker-build:
	docker build -t multi-source-ingestion .

docker-dry-run: docker-build
	docker run --rm -v "$$(pwd)/out:/app/out" multi-source-ingestion --dry-run
