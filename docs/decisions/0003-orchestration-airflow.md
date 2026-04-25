# ADR-0003: Use Apache Airflow for Orchestration

**Status:** Accepted
**Date:** 2026-04-22

## Context

The pipeline has a batch step (S3 → BigQuery + dbt run) that needs to run on a schedule, retry on transient failures, and surface errors to the operator. The streaming components (generator, Spark, Kafka) run continuously and don't need orchestration; only the batch portion does.

## Options Considered

| Option | Pros | Cons |
|---|---|---|
| **Apache Airflow** | Industry default, huge ecosystem, hireable skill | Heaviest of the options to run locally |
| Dagster | Better data-asset model, type system | Smaller ecosystem, more niche |
| Prefect | Cleaner Python API | Smaller market share than Airflow |
| Cron + bash | Trivial | No retries, no observability, no DAG semantics |
| GitHub Actions cron | Free, zero infra | Not designed for data pipelines, awkward state |

## Decision

Use **Apache Airflow 2.8** running locally via Docker Compose.

## Rationale

- **Hireability.** Airflow is the orchestrator that hiring managers in data engineering recognize first. For a portfolio project, picking the tool that the audience already speaks beats picking the technically-superior alternative.
- **Operator ecosystem.** `BashOperator`, `PythonOperator`, and provider packages for AWS / GCP cover everything the DAG needs without writing custom code.
- **Web UI** for triggering DAGs and inspecting failures is built-in — useful for the demo and matches what a real operator would have.
- **Maturity.** 2.8 is stable, well-documented, and the migration path to Cloud Composer or MWAA is essentially zero — same DAG code runs unchanged.

## Consequences

- The Airflow stack is heavier than the rest of the project (postgres + scheduler + webserver). The `airflow/docker-compose.yml` reflects that.
- Local Airflow is not HA; a single scheduler restart loses in-flight metadata. Acceptable for portfolio use — production would use Cloud Composer or MWAA. (See README "What I'd Improve at Production Scale".)
- Tasks in the DAG are coarse-grained (one task per pipeline stage). Dagster's asset-oriented model would make the data dependencies more explicit, but the cost of switching outweighs the benefit at this size.
