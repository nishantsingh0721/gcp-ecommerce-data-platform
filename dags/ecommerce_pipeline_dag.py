"""Coordinate the complete Bronze, Silver, and Gold e-commerce pipeline."""

from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocStartClusterOperator,
    DataprocStopClusterOperator,
)
from airflow.utils.trigger_rule import TriggerRule


PROJECT_ID = "gcp-ecommerce-de-dev"
REGION = "asia-south1"
CLUSTER_NAME = "ecom-dataproc-dev"
BATCH_DATE = "{{ dag_run.conf.get('batch_date', data_interval_end | ds) }}"

PIPELINE_TASK_IDS = (
    "start_dataproc",
    "trigger_bronze_pipeline",
    "trigger_silver_pipeline",
    "trigger_gold_pipeline",
    "stop_dataproc",
)


def verify_pipeline_status(**context):
    """Keep cleanup success from masking a failed pipeline stage."""
    task_instances = {
        task_instance.task_id: task_instance
        for task_instance in context["dag_run"].get_task_instances()
    }
    unsuccessful_tasks = [
        f"{task_id}={task_instances[task_id].state}"
        for task_id in PIPELINE_TASK_IDS
        if task_id in task_instances and task_instances[task_id].state != "success"
    ]
    if unsuccessful_tasks:
        raise AirflowException(
            "E-commerce pipeline failed or was incomplete: "
            + ", ".join(unsuccessful_tasks)
        )


with DAG(
    dag_id="ecommerce_pipeline",
    start_date=datetime(2026, 9, 1, tzinfo=timezone.utc),
    schedule=timedelta(days=3),
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
) as dag:
    start_dataproc = DataprocStartClusterOperator(
        task_id="start_dataproc",
        project_id=PROJECT_ID,
        region=REGION,
        cluster_name=CLUSTER_NAME,
    )

    trigger_bronze_pipeline = TriggerDagRunOperator(
        task_id="trigger_bronze_pipeline",
        trigger_dag_id="bronze_pipeline",
        conf={"batch_date": BATCH_DATE},
        logical_date="{{ logical_date }}",
        reset_dag_run=True,
        wait_for_completion=True,
        poke_interval=60,
    )

    trigger_silver_pipeline = TriggerDagRunOperator(
        task_id="trigger_silver_pipeline",
        trigger_dag_id="silver_pipeline",
        conf={"batch_date": BATCH_DATE},
        logical_date="{{ logical_date }}",
        reset_dag_run=True,
        wait_for_completion=True,
        poke_interval=60,
    )

    trigger_gold_pipeline = TriggerDagRunOperator(
        task_id="trigger_gold_pipeline",
        trigger_dag_id="gold_pipeline",
        conf={"batch_date": BATCH_DATE},
        logical_date="{{ logical_date }}",
        reset_dag_run=True,
        wait_for_completion=True,
        poke_interval=60,
    )

    stop_dataproc = DataprocStopClusterOperator(
        task_id="stop_dataproc",
        project_id=PROJECT_ID,
        region=REGION,
        cluster_name=CLUSTER_NAME,
        trigger_rule=TriggerRule.ALL_DONE,
        retries=2,
        retry_delay=timedelta(minutes=1),
    )

    verify_status = PythonOperator(
        task_id="verify_pipeline_status",
        python_callable=verify_pipeline_status,
        trigger_rule=TriggerRule.ALL_DONE,
        retries=0,
    )

    (
        start_dataproc
        >> trigger_bronze_pipeline
        >> trigger_silver_pipeline
        >> trigger_gold_pipeline
        >> stop_dataproc
        >> verify_status
    )
