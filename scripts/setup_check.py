import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import boto3

from google.cloud import bigquery
from kafka import KafkaConsumer

from config.config import (
    AWS_ACCESS_KEY_ID,
    AWS_REGION,
    AWS_SECRET_ACCESS_KEY,
    GCP_CREDENTIALS_PATH,
    GCP_PROJECT_ID,
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC,
    S3_BUCKET,
)


def check(label, fn):
    try:
        fn()
        print(f"  [PASS] {label}")
        return True
    except Exception as e:
        print(f"  [FAIL] {label}")
        print(f"         -> {e}")
        return False


def check_env_vars():
    missing = [
        name
        for name, val in {
            "AWS_ACCESS_KEY_ID": AWS_ACCESS_KEY_ID,
            "AWS_SECRET_ACCESS_KEY": AWS_SECRET_ACCESS_KEY,
            "S3_BUCKET": S3_BUCKET,
            "GCP_PROJECT_ID": GCP_PROJECT_ID,
            "GCP_CREDENTIALS_PATH": GCP_CREDENTIALS_PATH,
        }.items()
        if not val
    ]
    if missing:
        raise ValueError(f"Missing values in .env: {missing}")


def check_aws_s3():
    s3 = boto3.client(
        "s3",
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_REGION,
    )
    s3.head_bucket(Bucket=S3_BUCKET)


def check_kafka():
    consumer = KafkaConsumer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS, request_timeout_ms=5000)
    topics = consumer.topics()
    consumer.close()
    if KAFKA_TOPIC not in topics:
        raise ValueError(
            f"Topic '{KAFKA_TOPIC}' not found on broker. "
            "Run: python kafka/setup_topics.py"
        )


def check_gcp_bigquery():
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = GCP_CREDENTIALS_PATH
    client = bigquery.Client(project=GCP_PROJECT_ID)
    list(client.list_datasets())


def check_gcp_credentials_file():
    if not os.path.isfile(GCP_CREDENTIALS_PATH):
        raise FileNotFoundError(
            f"GCP key file not found at: {GCP_CREDENTIALS_PATH}\n"
            "Download it from GCP Console → IAM → Service Accounts → Keys."
        )


if __name__ == "__main__":
    print("=" * 50)
    print("  StreamFlow — Pre-flight Setup Check")
    print("=" * 50)

    results = [
        check(".env variables loaded", check_env_vars),
        check("GCP credentials file exists", check_gcp_credentials_file),
        check("AWS S3 bucket accessible", check_aws_s3),
        check("Kafka broker + topic ready", check_kafka),
        check("GCP BigQuery reachable", check_gcp_bigquery),
    ]

    print("=" * 50)
    passed = sum(results)
    total = len(results)
    print(f"  {passed}/{total} checks passed")
    print("=" * 50)

    if passed < total:
        print("\n  Fix the failing checks above, then re-run this script.")
        sys.exit(1)
    else:
        print("\n  All good! You are ready to run the pipeline.")
        sys.exit(0)
