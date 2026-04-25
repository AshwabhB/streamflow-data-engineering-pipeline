# ADR-0004: Use Apache Kafka as the Streaming Bus

**Status:** Accepted
**Date:** 2026-04-22

## Context

The generator produces transaction events continuously, and Spark consumes them on a different cadence (5-minute micro-batches). Decoupling the producer from the consumer requires a durable, ordered message broker.

## Options Considered

| Option | Pros | Cons |
|---|---|---|
| **Apache Kafka** | De-facto standard, durable, partitioned, ordered per key | Heavy to run locally |
| Redis Streams | Lighter weight, easy to run | Smaller ecosystem, less Spark-friendly |
| AWS Kinesis | Managed | Costs money, AWS-only, smaller skill market |
| GCP Pub/Sub | Managed, generous free tier | No native key-based partition ordering, GCP-only |
| RabbitMQ | Simple to set up | Built for messaging, not streaming — no log retention semantics |

## Decision

Use **Apache Kafka 7.5** (Confluent distribution) running in a single-broker Docker Compose stack.

## Rationale

- **Industry standard.** Kafka is the default streaming bus that any data engineer is expected to know. Picking it for a portfolio project is the safe and expected choice.
- **Per-key ordering.** Using `customer_id` as the message key guarantees all events for a given customer land on the same partition, in order. This matters for any downstream stateful processing (sessionization, fraud rule windows, etc.).
- **Spark Structured Streaming has first-class Kafka support** — no glue code needed beyond `format("kafka")`.
- **Same code runs on managed Kafka** (Confluent Cloud, MSK, Aiven). The producer and consumer don't change when moving from local to production.

## Consequences

- The local Kafka stack is single-broker — no replication, no fault tolerance. Fine for a development/portfolio environment, would be 3+ brokers across AZs in production.
- Confluent's image bundles Zookeeper rather than KRaft. For a real deployment in 2026 we'd switch to KRaft mode, but the local image keeps things simple.
- No Schema Registry. The event contract is enforced at code level via `schemas/transaction.schema.json` and Spark's `TRANSACTION_SCHEMA`. A real production deployment should add Confluent Schema Registry with Avro or Protobuf.
- 5-minute micro-batches are the latency floor for this design. Sub-second latency would need Spark Continuous Processing or a switch to Flink.
