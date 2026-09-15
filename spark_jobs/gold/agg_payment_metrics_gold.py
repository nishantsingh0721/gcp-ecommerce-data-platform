"""Recompute and merge affected agg_payment_metrics metric rows."""

import sys
from datetime import datetime

from google.cloud import bigquery
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, current_timestamp, lit, when
from pyspark.sql.functions import sum as spark_sum

PROJECT_ID = "gcp-ecommerce-de-dev"
FACT_PAYMENTS = f"{PROJECT_ID}.gold.fact_payments"
TARGET_TABLE = f"{PROJECT_ID}.gold.agg_payment_metrics"
AUDIT_TABLE = f"{PROJECT_ID}.gold.pipeline_audit"
OBJECT_NAME = "agg_payment_metrics"
PROCESSED_BY = "agg_payment_metrics_gold"
MERGE_KEYS = ("payment_method",)


def parse_batch_date():
    if len(sys.argv) != 2:
        raise ValueError(f"Usage: {sys.argv[0]} YYYY-MM-DD")
    try:
        return datetime.strptime(sys.argv[1], "%Y-%m-%d").date()
    except ValueError as exc:
        raise ValueError("batch_date must be a valid date in YYYY-MM-DD format") from exc


def ensure_audit_table(client):
    client.query(f"""CREATE TABLE IF NOT EXISTS `{AUDIT_TABLE}` (
        object_name STRING, batch_date DATE, status STRING,
        processed_at TIMESTAMP, message STRING
    )""").result()


def batch_already_succeeded(client, batch_date):
    config = bigquery.QueryJobConfig(query_parameters=[
        bigquery.ScalarQueryParameter("object_name", "STRING", OBJECT_NAME),
        bigquery.ScalarQueryParameter("batch_date", "DATE", batch_date),
    ])
    rows = client.query(
        f"""SELECT 1 FROM `{AUDIT_TABLE}`
        WHERE object_name = @object_name AND batch_date = @batch_date
          AND status = 'SUCCESS' LIMIT 1""",
        job_config=config,
    ).result()
    return next(iter(rows), None) is not None


def write_success_audit(client, batch_date):
    config = bigquery.QueryJobConfig(query_parameters=[
        bigquery.ScalarQueryParameter("object_name", "STRING", OBJECT_NAME),
        bigquery.ScalarQueryParameter("batch_date", "DATE", batch_date),
        bigquery.ScalarQueryParameter("message", "STRING", "INCREMENTAL_GOLD_MERGE_COMPLETED"),
    ])
    client.query(
        f"""INSERT INTO `{AUDIT_TABLE}`
        (object_name, batch_date, status, processed_at, message)
        VALUES (@object_name, @batch_date, 'SUCCESS', CURRENT_TIMESTAMP(), @message)""",
        job_config=config,
    ).result()


def build_metrics(spark, batch_date):
    facts = spark.read.format("bigquery").load(FACT_PAYMENTS).cache()
    affected = facts.filter(col("source_batch_date") == lit(batch_date)).select("payment_method").distinct()
    return (
        facts.join(affected, "payment_method", "inner").groupBy("payment_method")
        .agg(
            count("*").alias("total_payments"),
            spark_sum(when(col("payment_status") == "SUCCESS", 1).otherwise(0)).alias("successful_payments"),
            spark_sum(when(col("payment_status") == "FAILED", 1).otherwise(0)).alias("failed_payments"),
            spark_sum(when(col("payment_status") == "PENDING", 1).otherwise(0)).alias("pending_payments"),
            spark_sum(when(col("payment_status") == "SUCCESS", col("payment_amount")).otherwise(0)).alias("successful_payment_amount"),
            spark_sum("payment_amount").alias("total_payment_amount"),
        )
        .withColumn("success_rate", col("successful_payments") / col("total_payments"))
    )
    

def create_target_if_needed(client, staging_table):
    client.query(f"""CREATE TABLE IF NOT EXISTS `{TARGET_TABLE}`
        CLUSTER BY payment_method
        AS SELECT * FROM `{staging_table}` WHERE FALSE""").result()


def merge_metrics(client, staging_table, metric_columns):
    match_condition = " AND ".join(f"T.{key} = S.{key}" for key in MERGE_KEYS)
    update_columns = tuple(column for column in metric_columns if column not in MERGE_KEYS)
    update_clause = ", ".join(f"T.{column} = S.{column}" for column in update_columns)
    columns = ", ".join(metric_columns)
    values = ", ".join(f"S.{column}" for column in metric_columns)
    client.query(f"""MERGE `{TARGET_TABLE}` T USING `{staging_table}` S
        ON {match_condition}
        WHEN MATCHED AND S.last_processed_batch_date >= T.last_processed_batch_date
          THEN UPDATE SET {update_clause}
        WHEN NOT MATCHED THEN INSERT ({columns}) VALUES ({values})""").result()


def main():
    batch_date = parse_batch_date()
    staging_table = f"{PROJECT_ID}.gold.agg_payment_metrics_stage_{batch_date:%Y%m%d}"
    print(f"Processing Gold {OBJECT_NAME} for batch_date={batch_date}", flush=True)
    client = bigquery.Client(project=PROJECT_ID)
    ensure_audit_table(client)
    if batch_already_succeeded(client, batch_date):
        print(f"SKIPPED: {OBJECT_NAME} batch_date={batch_date} already processed successfully", flush=True)
        return

    spark = SparkSession.builder.appName("agg-payment-metrics-gold").getOrCreate()
    try:
        spark.conf.set("spark.sql.session.timeZone", "UTC")
        metrics = (
            build_metrics(spark, batch_date)
            .withColumn("metric_updated_timestamp", current_timestamp())
            .withColumn("last_processed_batch_date", lit(batch_date))
            .withColumn("processed_by", lit(PROCESSED_BY))
            .cache()
        )
        metric_count = metrics.count()
        distinct_count = metrics.select(*MERGE_KEYS).distinct().count()
        print(f"Metric row count: {metric_count}", flush=True)
        print(f"Distinct metric grain count: {distinct_count}", flush=True)
        if metric_count == 0:
            raise RuntimeError(f"No affected metric rows found for {OBJECT_NAME} batch_date={batch_date}")
        if distinct_count != metric_count:
            raise RuntimeError(f"{OBJECT_NAME} metric grain validation failed")

        attempted = False
        merged = False
        try:
            attempted = True
            metrics.write.format("bigquery").option("writeMethod", "direct").mode("overwrite").save(staging_table)
            create_target_if_needed(client, staging_table)
            merge_metrics(client, staging_table, tuple(metrics.columns))
            merged = True
        finally:
            if attempted:
                try:
                    client.delete_table(staging_table, not_found_ok=True)
                    print(f"Dropped staging table: {staging_table}", flush=True)
                except Exception as cleanup_error:
                    outcome = "after successful MERGE" if merged else ""
                    print(f"WARNING: staging cleanup failed {outcome}: {cleanup_error}", flush=True)

        write_success_audit(client, batch_date)
        print(f"Successfully merged Gold metrics: {TARGET_TABLE}", flush=True)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
