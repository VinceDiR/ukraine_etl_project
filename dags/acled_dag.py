""""ACLED Data Ingest DAG"""
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.glue_crawler import \
    GlueCrawlerOperator
from scripts.ingest_data import ingest_data

email = os.getenv("EMAIL_ADDRESS")

with DAG(
    dag_id="acled_data_ingest",
    start_date=datetime(2022, 10, 10),
    schedule_interval="@daily",
    catchup=True,
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
        "email_on_failure": False,
        "email_on_retry": False,
        "email": email,
    },
) as dag:

    start_task = EmptyOperator(task_id="acled_start_task", dag=dag)

    ingest_task = PythonOperator(
        task_id="acled_ingest_task",
        python_callable=ingest_data,
        op_args={"{{ds}}"},
        dag=dag,
    )
    glue_crawler_config = {
        'Name': 'acled-crawler',
    }

    crawler_task = GlueCrawlerOperator(
        task_id = "acled_crawler_task",
        config = glue_crawler_config,
        dag=dag,
    )

    end_task = EmptyOperator(task_id="acled_end_task", dag=dag)

    start_task >> ingest_task >> crawler_task >> end_task
