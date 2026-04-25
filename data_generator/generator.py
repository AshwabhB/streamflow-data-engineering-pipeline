import json
import os
import random
import sys
import time
import uuid
from datetime import datetime

from faker import Faker

from kafka import KafkaProducer

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.config import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC, TRANSACTIONS_PER_SECOND

fake = Faker()

PRODUCT_CATEGORIES = [
    "electronics",
    "clothing",
    "food",
    "books",
    "sports",
    "home",
    "beauty",
    "toys",
    "automotive",
    "jewelry",
]

TRANSACTION_STATUSES = ["success", "failed", "pending", "refunded"]
STATUS_WEIGHTS = [0.85, 0.07, 0.05, 0.03]


def create_producer():
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"),
        acks="all",
        retries=3,
    )


def generate_transaction():
    customer_id = f"cust_{random.randint(1, 10000):05d}"
    return {
        "transaction_id": f"txn_{uuid.uuid4().hex[:12]}",
        "customer_id": customer_id,
        "customer_name": fake.name(),
        "customer_email": fake.email(),
        "customer_country": fake.country_code(),
        "amount": round(random.uniform(5.0, 50000.0), 2),
        "currency": "USD",
        "status": random.choices(TRANSACTION_STATUSES, weights=STATUS_WEIGHTS)[0],
        "product_category": random.choice(PRODUCT_CATEGORIES),
        "product_name": fake.catch_phrase(),
        "quantity": random.randint(1, 10),
        "timestamp": datetime.now(datetime.UTC).isoformat(),
        "ip_address": fake.ipv4(),
        "device_type": random.choice(["mobile", "desktop", "tablet"]),
        "payment_method": random.choice(["credit_card", "debit_card", "paypal", "bank_transfer"]),
    }


def run_generator(producer, rate_per_second=TRANSACTIONS_PER_SECOND):
    print(f"Starting transaction generator: {rate_per_second} transactions/second")
    print(f"Publishing to Kafka topic: {KAFKA_TOPIC} at {KAFKA_BOOTSTRAP_SERVERS}")
    print("Press Ctrl+C to stop.\n")

    count = 0
    interval = 1.0 / rate_per_second

    while True:
        try:
            txn = generate_transaction()
            producer.send(KAFKA_TOPIC, key=txn["customer_id"], value=txn)
            count += 1
            if count % 100 == 0:
                print(f"[{datetime.now().strftime('%H:%M:%S')}] Published {count} transactions")
            time.sleep(interval)
        except KeyboardInterrupt:
            print(f"\nStopped. Total published: {count}")
            break
        except Exception as e:
            print(f"Error publishing transaction: {e}")
            time.sleep(1)


if __name__ == "__main__":
    producer = create_producer()
    try:
        run_generator(producer)
    finally:
        producer.flush()
        producer.close()
        print("Kafka producer closed.")
