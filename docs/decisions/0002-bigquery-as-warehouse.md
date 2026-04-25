# ADR-0002: Use BigQuery as the Analytics Warehouse

**Status:** Accepted
**Date:** 2026-04-22

## Context

The pipeline needs a serving layer where dbt can build dimensional models and where downstream consumers (analysts, dashboards, ML notebooks) can run ad-hoc SQL. The warehouse must:

- Run on free or near-free pricing for a portfolio project
- Scale to TB-class workloads without re-platforming
- Have first-class dbt support
- Be production-credible (i.e. something a real company would actually run)

## Options Considered

| Option | Free tier | dbt adapter | Notes |
|---|---|---|---|
| **BigQuery** | 1 TB query / 10 GB storage / month | `dbt-bigquery`, very mature | Serverless, no cluster to manage |
| Snowflake | 30-day trial only | `dbt-snowflake`, mature | Best dbt adapter but no permanent free tier |
| Redshift | None (unless free tier credits) | `dbt-redshift`, mature | Cluster management overhead |
| Databricks SQL | Community edition is limited | `dbt-databricks` | Would conflict with the open-source-Spark angle |
| DuckDB | Local, free | `dbt-duckdb` | Great for development, not credible as a "warehouse" |

## Decision

Use **Google BigQuery** with `dbt-bigquery` 1.7.

## Rationale

- **Free tier is generous and permanent.** 1 TB of query and 10 GB of storage per month is enough to run this project indefinitely without ever paying.
- **Serverless.** No cluster to size, suspend, or scale. Matches the "I'm one person, I want zero ops" reality of a portfolio project.
- **dbt-bigquery is one of the two most mature dbt adapters** (alongside Snowflake). All dbt features — incremental models, snapshots, tests, docs — work without caveats.
- **Credible at scale.** BigQuery serves Spotify, The New York Times, Twitter — picking it doesn't trigger "but would you use it in production?"
- **Already paired with GCP credentials** that engineers tend to have set up — this lowers the barrier for someone cloning the repo.

## Consequences

- The pipeline straddles two clouds (S3 in AWS, BigQuery in GCP). This is realistic — many companies run multi-cloud — but means two sets of credentials in `.env`.
- `bigquery/load_to_bq.py` does the S3 → BigQuery hop manually. A native federated query would be cleaner; left as a roadmap item.
- BigQuery's SQL dialect differs from PostgreSQL and Snowflake in small ways. Not an issue for the dbt models written here, but a future migration would touch SQL.
- Free-tier BigQuery has no query priority guarantees — fine for portfolio use, would need reservations in production.
