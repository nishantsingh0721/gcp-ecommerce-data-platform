"""Run all ten Bronze PySpark ingestion jobs on the existing Dataproc cluster."""

import sys
from datetime import datetime, timezone
from pathlib import Path


REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
if str(REPOSITORY_ROOT) not in sys.path:
    sys.path.insert(0, str(REPOSITORY_ROOT))

from airflow import DAG
from airflow.models.baseoperator import chain
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator

from scripts.prepare_source_batch import prepare_source_batch


PROJECT_ID = "gcp-ecommerce-de-dev"
REGION = "asia-south1"
CLUSTER_NAME = "ecom-dataproc-dev"
PYSPARK_BASE_URI = "gs://dataproc-staging-asia-south1-719421679095-mwznbteb/jobs"

# Use Airflow's logical run date so retries and future backfills remain consistent.
BATCH_DATE = "{{ ds }}"

BRONZE_TASK_IDS = (
    "customers_bronze",
    "products_bronze",
    "orders_bronze",
    "order_items_bronze",
    "payments_bronze",
    "shipments_bronze",
    "inventory_bronze",
    "returns_bronze",
    "promotions_bronze",
    "customer_events_bronze",
)


def build_job(task_id):
    """Describe the PySpark script and cluster for one Dataproc job."""
    return {
        "placement": {"cluster_name": CLUSTER_NAME},
        "pyspark_job": {
            # main_python_file_uri is the GCS location of the script to run.
            "main_python_file_uri": f"{PYSPARK_BASE_URI}/{task_id}.py",
            # Each Bronze script expects batch_date as its single positional argument.
            "args": [BATCH_DATE],
        },
    }


# This DAG represents the complete Landing-to-BigQuery Bronze workflow.
with DAG(
    dag_id="bronze_pipeline",
    start_date=datetime(2026, 9, 1, tzinfo=timezone.utc),
    schedule=None,  # Trigger manually during initial testing.
    catchup=False,  # Do not create runs for past scheduled intervals.
    max_active_runs=1,  # Prevent overlapping DAG runs during delete-then-append loads.
) as dag:
    prepare_task = PythonOperator(
        task_id="prepare_source_batch",
        python_callable=prepare_source_batch,
        op_kwargs={"batch_date": BATCH_DATE},
    )

    tasks = []
    for task_id in BRONZE_TASK_IDS:
        # Submit a job to the existing cluster and wait for it to finish.
        task = DataprocSubmitJobOperator(
            task_id=task_id,
            job=build_job(task_id),
            region=REGION,
            project_id=PROJECT_ID,
            asynchronous=False,
        )
        tasks.append(task)

    # Prepare Landing data first, then run each Bronze job sequentially.
    chain(prepare_task, *tasks)
