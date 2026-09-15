"""Run all ten incremental Silver PySpark jobs on the existing Dataproc cluster."""

from datetime import datetime, timezone

from airflow import DAG
from airflow.models.baseoperator import chain
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator


PROJECT_ID = "gcp-ecommerce-de-dev"
REGION = "asia-south1"
CLUSTER_NAME = "ecom-dataproc-dev"
PYSPARK_BASE_URI = (
    "gs://dataproc-staging-asia-south1-719421679095-mwznbteb/jobs/silver"
)
BATCH_DATE = "{{ dag_run.conf.get('batch_date', data_interval_end | ds) }}"

SILVER_TASK_IDS = (
    "customers_silver",
    "products_silver",
    "orders_silver",
    "order_items_silver",
    "payments_silver",
    "shipments_silver",
    "inventory_silver",
    "returns_silver",
    "promotions_silver",
    "customer_events_silver",
)


def build_job(task_id):
    """Describe the PySpark script and cluster for one Silver job."""
    return {
        "placement": {"cluster_name": CLUSTER_NAME},
        "pyspark_job": {
            "main_python_file_uri": f"{PYSPARK_BASE_URI}/{task_id}.py",
            "args": [BATCH_DATE],
        },
    }


# This DAG runs one logical Bronze-to-Silver batch.
with DAG(
    dag_id="silver_pipeline",
    start_date=datetime(2026, 9, 1, tzinfo=timezone.utc),
    schedule=None,  # Trigger manually during initial testing.
    catchup=False,
    max_active_runs=1,  # Prevent overlapping incremental Silver runs.
) as dag:
    tasks = []
    for task_id in SILVER_TASK_IDS:
        # Submit a job to the existing cluster and wait for it to finish.
        task = DataprocSubmitJobOperator(
            task_id=task_id,
            job=build_job(task_id),
            region=REGION,
            project_id=PROJECT_ID,
            asynchronous=False,
        )
        tasks.append(task)

    # Run each Silver job sequentially in the listed dependency order.
    chain(*tasks)
