# ADR-0001: Use Delta Lake on S3 as the Lakehouse Layer

**Status:** Accepted
**Date:** 2026-04-22

## Context

Spark Structured Streaming needs a durable sink for parsed transaction events. The sink must:

- Be cheap to store at GB-to-TB scale
- Survive Spark job restarts without corrupting data (ACID)
- Enforce schema so a buggy producer can't silently rot the table
- Be readable by external tools — BigQuery loaders, ad-hoc Spark, dbt

## Options Considered

| Option | Pros | Cons |
|---|---|---|
| Plain Parquet on S3 | Simplest, universal | No ACID, no schema enforcement, manual file registry |
| **Delta Lake on S3** | ACID, schema enforcement, time travel, open spec | Slightly heavier than raw Parquet |
| Apache Iceberg on S3 | Strong on partition evolution | Spark integration was less mature when this was built |
| Apache Hudi on S3 | Good for upserts | Overkill for an append-only workload |
| Hosted lakehouse (Databricks, Snowflake) | Managed | Vendor lock-in, costs money, hides the primitives |

## Decision

Use **Delta Lake 3.1 on S3**.

## Rationale

- **ACID writes** mean a crashed Spark job leaves the table in a known-good state. The `_delta_log` is the source of truth — partial Parquet files written before a crash are simply not committed.
- **Schema enforcement** rejects writes that don't match the declared schema. A future change to the generator that adds or removes a field fails loudly at the lakehouse boundary instead of silently producing corrupt analytics.
- **Time travel** (`VERSION AS OF`) is invaluable for debugging — "what did the table look like before yesterday's broken release?" is a one-line query.
- **Open specification.** Delta Lake is supported by Spark, Trino, BigQuery, and several other engines. No lock-in.
- **Vendor neutrality.** S3 is the lowest-common-denominator object store. The same code would run unchanged on GCS or Azure Blob with `s3a://` swapped for `gs://` or `wasbs://`.

## Consequences

- Spark jobs need the `delta-spark` package and the Delta SQL extension configured at session creation.
- Delta requires periodic `OPTIMIZE` and `VACUUM` to compact small files and clean up tombstones. Not done in this project; would be an Airflow weekly task in production.
- Reading Delta from BigQuery requires either an external table over the Parquet files (ignoring the transaction log) or going through a loader that handles Delta semantics. This project takes the loader approach in `bigquery/load_to_bq.py`.
