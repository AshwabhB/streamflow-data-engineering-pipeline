.PHONY: help install lint format test up down generate stream setup-check dbt-run dbt-test dbt-parse clean

help: ## Show this help message
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}'

install: ## Install runtime + dev dependencies and pre-commit hooks
	pip install -r requirements.txt
	pip install -r requirements-dev.txt
	pre-commit install

lint: ## Run all linters (Python + SQL)
	ruff check .
	ruff format --check .
	sqlfluff lint dbt/streamflow/models --dialect bigquery

format: ## Auto-fix lint issues
	ruff check --fix .
	ruff format .
	sqlfluff fix dbt/streamflow/models --dialect bigquery

test: ## Run pytest
	pytest -v

up: ## Start Kafka and Airflow stacks
	docker compose -f kafka/docker-compose.yml up -d
	docker compose -f airflow/docker-compose.yml up -d

down: ## Stop Kafka and Airflow stacks
	docker compose -f kafka/docker-compose.yml down
	docker compose -f airflow/docker-compose.yml down

generate: ## Run the transaction generator
	python data_generator/generator.py

stream: ## Run the Spark streaming job
	python spark/streaming_job.py

setup-check: ## Verify AWS / GCP / Kafka connectivity
	python scripts/setup_check.py

dbt-parse: ## Validate dbt models compile (no warehouse needed)
	cd dbt/streamflow && dbt parse --profiles-dir .

dbt-run: ## Run all dbt models against BigQuery
	cd dbt/streamflow && dbt run --profiles-dir .

dbt-test: ## Run dbt schema tests
	cd dbt/streamflow && dbt test --profiles-dir .

clean: ## Remove caches and dbt artifacts
	find . -type d -name "__pycache__" -exec rm -rf {} +
	find . -type d -name ".pytest_cache" -exec rm -rf {} +
	find . -type d -name ".ruff_cache" -exec rm -rf {} +
	rm -rf dbt/streamflow/target dbt/streamflow/dbt_packages dbt/streamflow/logs
