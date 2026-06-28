import logging
import os
import sys
import tempfile

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import boto3
from google.cloud import bigquery
from google.cloud.exceptions import NotFound

from config.config import (
    AWS_ACCESS_KEY_ID,
    AWS_REGION,
    AWS_SECRET_ACCESS_KEY,
    BIGQUERY_DATASET,
    BIGQUERY_LOCATION,
    GCP_CREDENTIALS_PATH,
    GCP_PROJECT_ID,
    S3_BUCKET,
    S3_PREFIX,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

RAW_DATASET = f"{BIGQUERY_DATASET}_raw"
TABLE_NAME = "transactions"
STAGING_TABLE = "transactions_staging"


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


def reset_staging_table(client):
    """Drop staging table so each run starts from a clean slate."""
    staging_ref = f"{GCP_PROJECT_ID}.{RAW_DATASET}.{STAGING_TABLE}"
    client.delete_table(staging_ref, not_found_ok=True)
    log.info("Staging table reset: %s", staging_ref)
    return staging_ref


def load_parquet_files_to_staging(client, staging_ref):
    """Download each Parquet file from S3 and append it to the staging table."""
    s3 = boto3.client(
        "s3",
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_REGION,
    )

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

            log.info("Staging s3://%s/%s → %s", S3_BUCKET, key, staging_ref)

            with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
                s3.download_fileobj(S3_BUCKET, key, tmp)
                tmp_path = tmp.name

            try:
                with open(tmp_path, "rb") as f:
                    job = client.load_table_from_file(f, staging_ref, job_config=job_config)
                    job.result()
                loaded += 1
            finally:
                os.unlink(tmp_path)

    log.info(
        "Staging complete. Loaded %d Parquet files. Skipped %d non-Parquet objects.",
        loaded,
        skipped,
    )
    return loaded


def merge_staging_to_main(client, staging_ref):
    """
    Upsert from staging into the main transactions table.
    On first run the main table won't exist yet — copy staging directly.
    On subsequent runs, MERGE ensures only new transaction_ids are inserted
    so retried DAG runs never produce duplicate rows.
    """
    table_ref = f"{GCP_PROJECT_ID}.{RAW_DATASET}.{TABLE_NAME}"

    try:
        client.get_table(table_ref)
        table_exists = True
    except NotFound:
        table_exists = False

    if not table_exists:
        log.info("Main table does not exist yet — copying staging as initial load.")
        copy_job = client.copy_table(staging_ref, table_ref)
        copy_job.result()
        rows = client.get_table(table_ref).num_rows
        log.info("Initial load complete: %d rows in %s", rows, table_ref)
    else:
        merge_sql = f"""
            MERGE `{table_ref}` AS target
            USING `{staging_ref}` AS source
            ON target.transaction_id = source.transaction_id
            WHEN NOT MATCHED THEN INSERT ROW
        """
        job = client.query(merge_sql)
        job.result()
        log.info(
            "MERGE complete: %d new rows inserted, duplicates skipped.",
            job.num_dml_affected_rows,
        )

    client.delete_table(staging_ref, not_found_ok=True)
    log.info("Staging table dropped.")


if __name__ == "__main__":
    log.info(
        "Starting BigQuery load: s3://%s/%s → %s.%s.%s",
        S3_BUCKET,
        S3_PREFIX,
        GCP_PROJECT_ID,
        RAW_DATASET,
        TABLE_NAME,
    )

    client = get_bq_client()
    ensure_dataset(client)

    staging_ref = reset_staging_table(client)
    loaded = load_parquet_files_to_staging(client, staging_ref)

    if loaded > 0:
        merge_staging_to_main(client, staging_ref)
    else:
        log.info("No Parquet files found — nothing to merge.")

    log.info("Load complete. Next: run  dbt run  to build staging and mart models.")
