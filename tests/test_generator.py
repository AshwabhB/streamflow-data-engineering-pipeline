from datetime import datetime

import pytest

from data_generator.generator import (
    PRODUCT_CATEGORIES,
    TRANSACTION_STATUSES,
    generate_transaction,
)

EXPECTED_FIELDS = {
    "transaction_id",
    "customer_id",
    "customer_name",
    "customer_email",
    "customer_country",
    "amount",
    "currency",
    "status",
    "product_category",
    "product_name",
    "quantity",
    "timestamp",
    "ip_address",
    "device_type",
    "payment_method",
}


def test_event_has_exactly_the_expected_fields():
    txn = generate_transaction()
    assert set(txn.keys()) == EXPECTED_FIELDS


def test_transaction_id_format():
    txn = generate_transaction()
    assert txn["transaction_id"].startswith("txn_")
    assert len(txn["transaction_id"]) == 16


def test_customer_id_format():
    txn = generate_transaction()
    assert txn["customer_id"].startswith("cust_")
    assert len(txn["customer_id"]) == 10


@pytest.mark.parametrize("_", range(50))
def test_amount_is_strictly_positive_float(_):
    txn = generate_transaction()
    assert isinstance(txn["amount"], float)
    assert txn["amount"] > 0
    assert txn["amount"] <= 50_000.0


@pytest.mark.parametrize("_", range(50))
def test_quantity_is_in_range(_):
    txn = generate_transaction()
    assert isinstance(txn["quantity"], int)
    assert 1 <= txn["quantity"] <= 10


def test_status_is_within_enum():
    for _ in range(200):
        txn = generate_transaction()
        assert txn["status"] in TRANSACTION_STATUSES


def test_status_distribution_includes_success():
    statuses = {generate_transaction()["status"] for _ in range(200)}
    assert "success" in statuses


def test_product_category_is_within_enum():
    for _ in range(50):
        txn = generate_transaction()
        assert txn["product_category"] in PRODUCT_CATEGORIES


def test_currency_is_usd():
    assert generate_transaction()["currency"] == "USD"


def test_device_type_is_known():
    allowed = {"mobile", "desktop", "tablet"}
    for _ in range(30):
        assert generate_transaction()["device_type"] in allowed


def test_payment_method_is_known():
    allowed = {"credit_card", "debit_card", "paypal", "bank_transfer"}
    for _ in range(30):
        assert generate_transaction()["payment_method"] in allowed


def test_timestamp_is_iso8601_with_timezone():
    txn = generate_transaction()
    parsed = datetime.fromisoformat(txn["timestamp"])
    assert parsed.tzinfo is not None


def test_email_contains_at_sign():
    txn = generate_transaction()
    assert "@" in txn["customer_email"]


def test_country_is_two_letter_code():
    txn = generate_transaction()
    assert len(txn["customer_country"]) == 2
    assert txn["customer_country"].isalpha()


def test_events_are_unique_across_calls():
    ids = {generate_transaction()["transaction_id"] for _ in range(500)}
    assert len(ids) == 500
