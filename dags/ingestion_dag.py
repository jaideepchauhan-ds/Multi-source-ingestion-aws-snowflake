"""Airflow DAG: schedules ONLY the extract/transform/validate/upload stage.

Loading into Snowflake is event-driven (S3 -> SQS -> Snowpipe -> Streams/Tasks), so Airflow does not
need warehouse access, and a slow warehouse can never block ingestion. One task per source gives
independent retries and failure isolation. Mount the repo at /opt/airflow/project (or edit the path).
"""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

SOURCES = ("fx_rates", "mf_nav", "books")

default_args = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,  # failures alert through SNS inside the pipeline
}

with DAG(
    dag_id="multi_source_ingestion",
    description="API + file download + web scraping -> S3 (Snowpipe loads from there)",
    start_date=datetime(2025, 1, 1),
    schedule="0 2 * * *",  # 02:00 UTC daily
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["ingestion", "aws", "snowflake"],
) as dag:
    for source in SOURCES:
        BashOperator(
            task_id=f"ingest_{source}",
            bash_command=(
                "cd /opt/airflow/project && "
                f"python -m ingestion.main --sources {source} --run-date {{{{ ds }}}}"
            ),
            env={
                "S3_BUCKET": "{{ var.value.ingest_s3_bucket }}",
                "AWS_REGION": "{{ var.value.get('aws_region', 'ap-south-1') }}",
                "SNS_TOPIC_ARN": "{{ var.value.get('ingest_sns_topic_arn', '') }}",
            },
            append_env=True,
        )
