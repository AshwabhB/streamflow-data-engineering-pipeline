import json

import pytest
from pyspark.sql import Row, SparkSession

from spark.streaming_job import build_dlq_payload, parse_transactions, split_valid_invalid

VALID_TXN = {
    "transaction_id": "txn_1234567890ab",
    "customer_id": "cust_abcd12",
    "customer_name": "Jane Doe",
    "customer_email": "jane@example.com",
    "customer_country": "US",
    "amount": 99.50,
    "currency": "USD",
    "status": "success",
    "product_category": "electronics",
    "product_name": "Headphones",
    "quantity": 1,
    "timestamp": "2026-01-01T12:00:00+00:00",
    "ip_address": "10.0.0.1",
    "device_type": "mobile",
    "payment_method": "credit_card",
}


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder.master("local[2]")
        .appName("streamflow-tests")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    yield session
    session.stop()


def kafka_df(spark, *payloads):
    """Build a DataFrame mimicking Kafka's raw `value` column (one row per payload)."""
    rows = [
        Row(value=(p if isinstance(p, str) else json.dumps(p)).encode("utf-8")) for p in payloads
    ]
    return spark.createDataFrame(rows)


def test_parse_transactions_extracts_all_fields(spark):
    parsed = parse_transactions(kafka_df(spark, VALID_TXN))
    row = parsed.collect()[0]

    assert row["transaction_id"] == VALID_TXN["transaction_id"]
    assert row["customer_id"] == VALID_TXN["customer_id"]
    assert row["amount"] == VALID_TXN["amount"]
    assert row["status"] == "success"


def test_parse_transactions_preserves_raw_value(spark):
    parsed = parse_transactions(kafka_df(spark, VALID_TXN))
    row = parsed.collect()[0]

    assert json.loads(row["raw_value"]) == VALID_TXN


def test_parse_transactions_derives_time_partition_columns(spark):
    parsed = parse_transactions(kafka_df(spark, VALID_TXN))
    row = parsed.collect()[0]

    assert row["year"] == 2026
    assert row["month"] == 1
    assert row["day"] == 1
    assert row["hour"] == 12


def test_valid_transaction_has_no_dlq_reason(spark):
    parsed = parse_transactions(kafka_df(spark, VALID_TXN))
    row = parsed.collect()[0]

    assert row["dlq_reason"] is None


def test_missing_transaction_id_flagged_for_dlq(spark):
    bad = {**VALID_TXN, "transaction_id": None}
    parsed = parse_transactions(kafka_df(spark, bad))
    row = parsed.collect()[0]

    assert row["dlq_reason"] == "missing_transaction_id"


def test_negative_amount_flagged_for_dlq(spark):
    bad = {**VALID_TXN, "amount": -10.0}
    parsed = parse_transactions(kafka_df(spark, bad))
    row = parsed.collect()[0]

    assert row["dlq_reason"] == "invalid_amount"


def test_zero_amount_flagged_for_dlq(spark):
    bad = {**VALID_TXN, "amount": 0.0}
    parsed = parse_transactions(kafka_df(spark, bad))
    row = parsed.collect()[0]

    assert row["dlq_reason"] == "invalid_amount"


def test_malformed_json_flagged_for_dlq(spark):
    parsed = parse_transactions(kafka_df(spark, "{not valid json"))
    row = parsed.collect()[0]

    assert row["dlq_reason"] == "missing_transaction_id"


def test_split_valid_invalid_separates_rows_correctly(spark):
    bad = {**VALID_TXN, "transaction_id": "txn_aaaaaaaaaaaa", "amount": -5.0}
    parsed = parse_transactions(kafka_df(spark, VALID_TXN, bad))

    valid, invalid = split_valid_invalid(parsed)

    assert valid.count() == 1
    assert invalid.count() == 1
    assert "raw_value" not in valid.columns
    assert "dlq_reason" not in valid.columns


def test_build_dlq_payload_wraps_raw_value_and_reason(spark):
    bad = {**VALID_TXN, "amount": -1.0}
    parsed = parse_transactions(kafka_df(spark, bad))
    _, invalid = split_valid_invalid(parsed)

    payload_row = build_dlq_payload(invalid).collect()[0]
    payload = json.loads(payload_row["value"])

    assert payload["error_reason"] == "invalid_amount"
    assert json.loads(payload["raw_value"])["transaction_id"] == bad["transaction_id"]
    assert "error_timestamp" in payload
