# StreamFlow

> **Real-time e-commerce data pipeline: Kafka → Spark Structured Streaming → Delta Lake on S3 → Airflow → dbt → BigQuery.**

[![CI](https://github.com/AshwabhB/streamflow-data-engineering-pipeline/actions/workflows/ci.yml/badge.svg)](https://github.com/AshwabhB/streamflow-data-engineering-pipeline/actions/workflows/ci.yml)
[![CodeQL](https://github.com/AshwabhB/streamflow-data-engineering-pipeline/actions/workflows/codeql.yml/badge.svg)](https://github.com/AshwabhB/streamflow-data-engineering-pipeline/actions/workflows/codeql.yml)

---

## Overview

A Faker-based producer publishes synthetic transaction events to a local Kafka topic. A Spark Structured Streaming job consumes them, validates against a typed schema, partitions by event time, and writes Delta Lake tables to S3. Airflow runs a scheduled DAG that loads new Parquet from S3 into BigQuery, then triggers dbt to materialize cleaned staging views and the `dim_customers` / `fct_transactions` marts.

---

## Architecture

```
┌──────────────┐    ┌────────┐    ┌──────────────────┐    ┌─────────────┐
│  Generator   │───▶│ Kafka  │───▶│ Spark Streaming  │───▶│  S3 + Delta │
│ (Faker, py)  │    │ (local)│    │   (PySpark)      │    │    Lake     │
└──────────────┘    └────────┘    └──────────────────┘    └──────┬──────┘
                                                                 │
                                                                 ▼
                          ┌────────────┐    ┌────────────────────────┐
                          │  Airflow   │───▶│  load_to_bq.py + dbt   │
                          │  (DAGs)    │    │   (BigQuery models)    │
                          └────────────┘    └────────────────────────┘
                                                                 │
                                                                 ▼
                                                       ┌──────────────────┐
                                                       │ BigQuery: raw,   │
                                                       │ staging, marts   │
                                                       └──────────────────┘
```

Detailed write-up in [docs/architecture.md](docs/architecture.md). Per-tool rationale in [docs/decisions/](docs/decisions/).

---

## Tech Stack

| Layer | Tool |
|---|---|
| Generation | Python + Faker |
| Streaming bus | Apache Kafka 7.5 |
| Stream processing | PySpark 3.5 (Structured Streaming) |
| Lakehouse | Delta Lake 3.1 on S3 |
| Orchestration | Apache Airflow 2.8 |
| Transformation | dbt-bigquery 1.7 |
| Warehouse | Google BigQuery |
| CI/CD | GitHub Actions |
| Quality | ruff, sqlfluff, pre-commit |

---

## Project Structure

```
.
├── .github/
│   ├── workflows/         # CI, CodeQL
│   ├── dependabot.yml
│   └── PULL_REQUEST_TEMPLATE.md
├── data_generator/        # Faker-based transaction producer
├── kafka/                 # Local Kafka + Zookeeper via Docker
├── spark/                 # Structured Streaming → Delta on S3
├── airflow/               # Airflow stack + DAGs
├── bigquery/              # S3 → BigQuery loader + analytics SQL
├── dbt/streamflow/        # dbt project (staging + marts)
├── config/                # Centralised env-var loading
├── schemas/               # JSON Schema for the event contract
├── scripts/               # Connectivity smoke test
├── tests/                 # pytest unit tests
├── docs/
│   ├── architecture.md
│   └── decisions/         # Architecture Decision Records
├── Makefile
├── requirements.txt
├── requirements-dev.txt
└── .env.example
```

---

## Prerequisites

- Python 3.11+
- Docker Desktop
- AWS account with an S3 bucket (free tier is sufficient)
- GCP account with BigQuery enabled (free tier is sufficient)

---

## Setup

```bash
git clone https://github.com/AshwabhB/streamflow-data-engineering-pipeline.git
cd streamflow-data-engineering-pipeline
make install           # installs runtime + dev deps and pre-commit hooks
cp .env.example .env   # then fill in AWS + GCP credentials
make setup-check       # verifies cloud connectivity
```

`make help` lists every available target.

---

## Running the Pipeline

Open separate terminals — each component is long-running.

```bash
make up                # starts Kafka and Airflow via Docker
make generate          # terminal 2: starts the producer
make stream            # terminal 3: starts the Spark consumer
```

The Airflow UI is at `http://localhost:8080`; trigger `pipeline_dag` once the stream has produced data. dbt models can also be run directly:

```bash
make dbt-run
make dbt-test
```

Stop everything with `make down`.

---

## Continuous Integration

Every push and PR runs:

| Workflow | Job | What it does |
|---|---|---|
| `ci.yml` | `python-lint` | `ruff check` + `ruff format --check` |
| `ci.yml` | `sql-lint` | `sqlfluff lint` against the BigQuery dialect |
| `ci.yml` | `dbt-parse` | Validates dbt models compile without a live warehouse |
| `ci.yml` | `tests` | `pytest` on the event generator |
| `ci.yml` | `validate-compose` | `docker compose config` on Kafka + Airflow stacks |
| `codeql.yml` | `analyze` | CodeQL static analysis for Python |

Local pre-commit hooks (`make install` configures them) run the same lint suite before every commit.

---

## Testing

```bash
make test            # pytest
make lint            # ruff + sqlfluff
make format          # auto-fix style issues
```

Unit tests live in `tests/` and cover the event generator's contract — every emitted event is verified against the schema fields, status enum, amount range, and timestamp format expected downstream by Spark. dbt schema tests live alongside the models in `dbt/streamflow/models/**/schema.yml`.

---

## Roadmap

- [ ] Spark unit tests with `pytest-spark`
- [ ] Fraud-detection ML step reading from `fct_transactions`
- [ ] Terraform module for the AWS S3 bucket and BigQuery datasets
- [ ] Cloud Composer variant of the Airflow stack
- [ ] OpenLineage instrumentation
- [ ] Streamlit dashboard backed by BigQuery
