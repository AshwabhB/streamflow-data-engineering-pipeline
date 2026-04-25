from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import logging

log = logging.getLogger(__name__)

default_args = {
    "owner": "streamflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

DBT_DIR = "/opt/airflow/dbt/streamflow"
BQ_SCRIPT = "/opt/airflow/scripts/bigquery/load_to_bq.py"


def log_pipeline_start(**context):
    log.info("StreamFlow pipeline run started: %s", context["ts"])


with DAG(
    dag_id="streamflow_pipeline",
    default_args=default_args,
    description="Hourly orchestration: S3 check → dbt → BigQuery load",
    schedule_interval="@hourly",
    start_date=days_ago(1),
    catchup=False,
    tags=["streamflow", "ecommerce", "dbt", "bigquery"],
) as dag:

    start = PythonOperator(
        task_id="log_start",
        python_callable=log_pipeline_start,
    )

    check_s3_data = BashOperator(
        task_id="check_s3_data",
        bash_command=(
            "aws s3 ls s3://$S3_BUCKET/transactions/ --recursive "
            "| tail -10 && echo 'S3 data check passed'"
        ),
    )

    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command=f"cd {DBT_DIR} && dbt deps",
    )

    dbt_run_staging = BashOperator(
        task_id="dbt_run_staging",
        bash_command=f"cd {DBT_DIR} && dbt run --select staging",
    )

    dbt_test_staging = BashOperator(
        task_id="dbt_test_staging",
        bash_command=f"cd {DBT_DIR} && dbt test --select staging",
    )

    dbt_run_marts = BashOperator(
        task_id="dbt_run_marts",
        bash_command=f"cd {DBT_DIR} && dbt run --select marts",
    )

    dbt_test_marts = BashOperator(
        task_id="dbt_test_marts",
        bash_command=f"cd {DBT_DIR} && dbt test --select marts",
    )

    load_to_bigquery = BashOperator(
        task_id="load_to_bigquery",
        bash_command=f"python {BQ_SCRIPT}",
    )

    # Pipeline dependency chain
    (
        start
        >> check_s3_data
        >> dbt_deps
        >> dbt_run_staging
        >> dbt_test_staging
        >> dbt_run_marts
        >> dbt_test_marts
        >> load_to_bigquery
    )
