import os
import sys
import logging
import tempfile

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import boto3
from google.cloud import bigquery
from google.cloud.exceptions import NotFound

from config.config import (
    GCP_PROJECT_ID, GCP_CREDENTIALS_PATH, BIGQUERY_DATASET, BIGQUERY_LOCATION,
    AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION,
    S3_BUCKET, S3_PREFIX,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

RAW_DATASET = f"{BIGQUERY_DATASET}_raw"
TABLE_NAME = "transactions"


def get_bq_client():
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = GCP_CREDENTIALS_PATH
    return bigquery.Client(project=GCP_PROJECT_ID)


def ensure_dataset(client):
    dataset_ref = f"{GCP_PROJECT_ID}.{RAW_DATASET}"
    try:
        client.get_dataset(dataset_ref)
        log.info("Dataset %s already exists", dataset_ref)
    except NotFound:
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = BIGQUERY_LOCATION
        client.create_dataset(dataset)
        log.info("Created dataset %s", dataset_ref)
    return dataset_ref


def load_parquet_files_from_s3(client, dataset_ref):
    s3 = boto3.client(
        "s3",
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_REGION,
    )

    table_ref = f"{GCP_PROJECT_ID}.{RAW_DATASET}.{TABLE_NAME}"

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        autodetect=True,
    )

    paginator = s3.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=S3_BUCKET, Prefix=S3_PREFIX)

    loaded = 0
    skipped = 0

    for page in pages:
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if not key.endswith(".parquet"):
                skipped += 1
                continue

            log.info("Loading s3://%s/%s → %s", S3_BUCKET, key, table_ref)

            with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
                s3.download_fileobj(S3_BUCKET, key, tmp)
                tmp_path = tmp.name

            try:
                with open(tmp_path, "rb") as f:
                    job = client.load_table_from_file(f, table_ref, job_config=job_config)
                    job.result()
                loaded += 1
            finally:
                os.unlink(tmp_path)

    log.info("Done. Loaded %d Parquet files. Skipped %d non-Parquet objects.", loaded, skipped)
    return loaded


if __name__ == "__main__":
    log.info("Starting BigQuery load: s3://%s/%s → %s.%s.%s",
             S3_BUCKET, S3_PREFIX, GCP_PROJECT_ID, RAW_DATASET, TABLE_NAME)

    client = get_bq_client()
    dataset_ref = ensure_dataset(client)
    count = load_parquet_files_from_s3(client, dataset_ref)

    log.info("Load complete. %d files ingested.", count)
    log.info("Next: run  dbt run  to build staging and mart models.")
