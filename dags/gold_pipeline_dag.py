"""Run all fifteen incremental Gold PySpark jobs on the existing Dataproc cluster."""

from datetime import datetime, timezone

from airflow import DAG
from airflow.models.baseoperator import chain
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator


PROJECT_ID = "gcp-ecommerce-de-dev"
REGION = "asia-south1"
CLUSTER_NAME = "ecom-dataproc-dev"
PYSPARK_BASE_URI = (
    "gs://dataproc-staging-asia-south1-719421679095-mwznbteb/jobs/gold"
)
BATCH_DATE = "{{ dag_run.conf.get('batch_date', ds) }}"

# Core Gold models establish dimensions and transaction/snapshot facts.
CORE_GOLD_TASK_IDS = (
    "dim_customer_current_gold",
    "dim_product_current_gold",
    "fact_sales_gold",
    "fact_payments_gold",
    "fact_shipments_gold",
    "fact_returns_gold",
    "fact_inventory_snapshot_gold",
    "fact_customer_events_gold",
)

# Gold KPI and metric models run only after every core model succeeds.
GOLD_METRIC_TASK_IDS = (
    "agg_daily_sales_gold",
    "agg_customer_metrics_gold",
    "agg_product_performance_gold",
    "agg_payment_metrics_gold",
    "agg_shipment_metrics_gold",
    "agg_return_metrics_gold",
    "agg_inventory_metrics_gold",
)

GOLD_TASK_IDS = CORE_GOLD_TASK_IDS + GOLD_METRIC_TASK_IDS


def build_job(task_id):
    """Describe the PySpark script and cluster for one Gold job."""
    return {
        "placement": {"cluster_name": CLUSTER_NAME},
        "pyspark_job": {
            "main_python_file_uri": f"{PYSPARK_BASE_URI}/{task_id}.py",
            "args": [BATCH_DATE],
        },
    }


# This DAG runs one logical Silver-to-Gold batch.
with DAG(
    dag_id="gold_pipeline",
    start_date=datetime(2026, 9, 1, tzinfo=timezone.utc),
    schedule=None,  # Trigger manually during initial testing.
    catchup=False,
    max_active_runs=1,  # Prevent overlapping incremental Gold runs.
) as dag:
    tasks = []
    for task_id in GOLD_TASK_IDS:
        # Submit a job to the existing cluster and wait for it to finish.
        task = DataprocSubmitJobOperator(
            task_id=task_id,
            job=build_job(task_id),
            region=REGION,
            project_id=PROJECT_ID,
            asynchronous=False,
        )
        tasks.append(task)

    # Core models run first; KPI models follow in the same sequential chain.
    chain(*tasks)
