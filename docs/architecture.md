# Architecture

This document describes how StreamFlow is put together and the rationale behind each layer. For decision-by-decision rationale, see [decisions/](decisions/).

---

## High-Level Flow

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

---

## Layer-by-Layer

### 1. Event Generation — `data_generator/generator.py`

A Python process built on `Faker` that emits synthetic transaction events at a configurable rate (default 10/sec). Every event conforms to [`schemas/transaction.schema.json`](../schemas/transaction.schema.json).

**Key design points:**

- `customer_id` is used as the Kafka message **key**, guaranteeing all events for the same customer land on the same partition (preserves per-customer ordering downstream).
- Producer is configured with `acks="all"` and `retries=3` to make publishing resilient to transient broker failures.
- Status values follow a weighted distribution (85% success, 7% failed, 5% pending, 3% refunded) — realistic enough for analytics to look meaningful.

### 2. Streaming Bus — Kafka

A single-broker Kafka cluster running in Docker via `kafka/docker-compose.yml`. The `transactions` topic is created with `setup_topics.py`.

In a real deployment this would be a managed multi-AZ Kafka (Confluent Cloud, MSK) with at least 3 partitions per topic. For the portfolio it's local — the *interface* is what matters.

### 3. Stream Processing — `spark/streaming_job.py`

PySpark Structured Streaming reads from Kafka, parses each JSON value against `TRANSACTION_SCHEMA`, derives partition columns from the event timestamp, filters out malformed rows, and writes to a Delta Lake table on S3.

**Key design points:**

- **Checkpointing** to `SPARK_CHECKPOINT_DIR` enables exactly-once delivery on restart.
- **Time-based partitioning** by `year/month/day` keeps file sizes manageable and enables partition pruning in BigQuery later.
- **Filter on `amount > 0` and non-null `transaction_id`** at the streaming layer — bad data never enters the lakehouse. (At scale, route these to a dead-letter topic; see Roadmap.)
- **Trigger interval** is 5 minutes — micro-batch rather than continuous, which keeps S3 file counts low and matches the latency requirements of analytics workloads.

### 4. Lakehouse — Delta Lake on S3

Delta Lake provides:

- **ACID writes** so a Spark job restart can never corrupt the table
- **Schema enforcement** so a future generator change doesn't silently break downstream
- **Time travel and `OPTIMIZE`** for retroactive maintenance

S3 is the cheapest durable object store and is supported by every other tool in the stack. See [ADR-0001](decisions/0001-delta-lake-on-s3.md).

### 5. Orchestration — Airflow

A locally-hosted Airflow stack (`airflow/docker-compose.yml`) runs `airflow/dags/pipeline_dag.py`. The DAG:

1. Triggers `bigquery/load_to_bq.py` to load new Parquet files from S3 into the `streamflow_raw.transactions` table
2. Runs `dbt run` to materialise staging views and marts
3. Runs `dbt test` to enforce schema contracts

Retries and SLAs are set on each task. See [ADR-0003](decisions/0003-orchestration-airflow.md).

### 6. Transformation — dbt

Two layers, mirroring the standard "staging → marts" pattern:

- **`stg_transactions`** (view) — typed, cleaned, deduplicated
- **`dim_customers`** (table) — one row per customer with lifetime metrics
- **`fct_transactions`** (table) — transaction grain with customer attributes denormalized in

`schema.yml` files declare uniqueness, not-null, and accepted-values tests on every column that has a contract.

### 7. Analytics — BigQuery

BigQuery is the serving layer. `bigquery/analytics_queries.sql` contains representative business queries (revenue by category, top customers, daily KPIs). See [ADR-0002](decisions/0002-bigquery-as-warehouse.md).

---

## Configuration & Secrets

A single `.env` file (template: `.env.example`) holds every environment-specific value: AWS keys, GCP project, S3 bucket, dataset name, etc. `config/config.py` loads it once and exposes typed constants to the rest of the codebase. No file in the repo holds a secret — the `.gitignore` is explicit about `*.env`, `*.json` keyfiles, and `*.pem`.

---

## Quality & CI

- `ruff` + `sqlfluff` lint Python and SQL on every commit (locally via pre-commit, in CI on every push)
- `pytest` runs the generator contract tests
- `dbt parse` validates the dbt project compiles without needing a live warehouse
- `CodeQL` scans Python for security issues weekly and on every PR
- `docker compose config` validates the Compose files

The CI workflow is in [`.github/workflows/ci.yml`](../.github/workflows/ci.yml).

---

## What's Out of Scope

This is a portfolio project; production-grade concerns explicitly *not* addressed include:

- No Schema Registry — JSON Schema is the de-facto contract, not enforced at the broker
- No Terraform / IaC — cloud resources are created by hand following the README
- No multi-environment promotion — there's just `dev`
- No observability stack — logs go to stdout
- No data lineage — would use OpenLineage in a real system

Each of these would be a follow-up project worth its own PR.
