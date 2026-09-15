"""Incrementally standardize and validate one Bronze orders batch."""

import sys
from datetime import datetime

from google.api_core.exceptions import NotFound
from google.cloud import bigquery
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat_ws, current_timestamp, lit, row_number, trim, upper, when
from pyspark.sql.window import Window

SOURCE_TABLE = "gcp-ecommerce-de-dev.bronze.orders"
TARGET_TABLE = "gcp-ecommerce-de-dev.silver.orders"
QUARANTINE_PATH = "gs://gcp-ecommerce-de-dev-quarantine/orders/"
PROCESSED_BY = "orders_silver"
AUDIT_TABLE = "gcp-ecommerce-de-dev.silver.pipeline_audit"
DATASET_NAME = "orders"
VALID_STATUSES = ("PENDING", "CONFIRMED", "SHIPPED", "DELIVERED", "CANCELLED")
BUSINESS_COLUMNS = (
    "order_id", "customer_id", "order_date", "order_status", "shipping_city",
    "shipping_state", "order_total", "currency",
)


def parse_batch_date():
    if len(sys.argv) != 2:
        raise ValueError(f"Usage: {sys.argv[0]} YYYY-MM-DD")
    try:
        return datetime.strptime(sys.argv[1], "%Y-%m-%d").date()
    except ValueError as exc:
        raise ValueError("batch_date must be a valid date in YYYY-MM-DD format") from exc


def query_config(batch_date):
    return bigquery.QueryJobConfig(query_parameters=[bigquery.ScalarQueryParameter("batch_date", "DATE", batch_date)])


def ensure_audit_table(client):
    client.query(f"""CREATE TABLE IF NOT EXISTS `{AUDIT_TABLE}` (dataset_name STRING, batch_date DATE, status STRING, processed_at TIMESTAMP, message STRING)""").result()


def batch_exists(client, table_name, batch_date, *, successful_only=False):
    try:
        client.get_table(table_name)
    except NotFound:
        return False
    if table_name == AUDIT_TABLE:
        config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("dataset_name", "STRING", DATASET_NAME),
                bigquery.ScalarQueryParameter("batch_date", "DATE", batch_date),
            ]
        )
        sql = f"""SELECT 1 FROM `{AUDIT_TABLE}` WHERE dataset_name = @dataset_name AND batch_date = @batch_date AND status = 'SUCCESS' LIMIT 1"""
    else:
        config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("batch_date", "DATE", batch_date),
            ]
        )
        sql = f"SELECT 1 FROM `{TARGET_TABLE}` WHERE batch_date = @batch_date LIMIT 1"
    return next(iter(client.query(sql, job_config=config).result()), None) is not None


def write_audit(client, batch_date, message):
    config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("dataset_name", "STRING", DATASET_NAME),
            bigquery.ScalarQueryParameter("batch_date", "DATE", batch_date),
            bigquery.ScalarQueryParameter("message", "STRING", message),
        ]
    )
    client.query(
        f"INSERT INTO `{AUDIT_TABLE}` (dataset_name, batch_date, status, processed_at, message) VALUES (@dataset_name, @batch_date, 'SUCCESS', CURRENT_TIMESTAMP(), @message)",
        job_config=config,
    ).result()


def delete_target_batch(client, batch_date):
    try:
        client.get_table(TARGET_TABLE)
    except NotFound:
        return
    client.query(f"DELETE FROM `{TARGET_TABLE}` WHERE batch_date = @batch_date", job_config=query_config(batch_date)).result()


def main():
    batch_date = parse_batch_date()
    print(f"Processing Silver orders for batch_date={batch_date}", flush=True)
    client = bigquery.Client(project=TARGET_TABLE.split(".")[0])
    ensure_audit_table(client)
    if batch_exists(client, AUDIT_TABLE, batch_date, successful_only=True):
        print(f"SKIPPED: {DATASET_NAME} batch_date={batch_date} already processed successfully", flush=True)
        return
    if batch_exists(client, TARGET_TABLE, batch_date):
        write_audit(client, batch_date, "BOOTSTRAPPED_EXISTING_SILVER_BATCH")
        print(f"SKIPPED: existing {DATASET_NAME} Silver batch_date={batch_date} was bootstrapped", flush=True)
        return

    spark = SparkSession.builder.appName("orders-silver").getOrCreate()
    try:
        spark.conf.set("spark.sql.session.timeZone", "UTC")
        print(f"Bronze source table: {SOURCE_TABLE}", flush=True)
        print(f"Silver target table: {TARGET_TABLE}", flush=True)
        print(f"Quarantine path: {QUARANTINE_PATH}", flush=True)
        bronze = (
            spark.read.format("bigquery")
            .option("filter", f"batch_date = '{batch_date}'")
            .load(SOURCE_TABLE)
            .filter(col("batch_date") == lit(batch_date))
        )
        if bronze.limit(1).count() == 0:
            raise RuntimeError(f"No Bronze {DATASET_NAME} rows found for batch_date={batch_date}")
        standardized = (
            bronze.withColumn("order_id", trim(col("order_id")))
            .withColumn("customer_id", trim(col("customer_id")))
            .withColumn("order_status", upper(trim(col("order_status"))))
            .withColumn("shipping_city", trim(col("shipping_city")))
            .withColumn("shipping_state", trim(col("shipping_state")))
            .withColumn("currency", upper(trim(col("currency"))))
        )
        window = Window.partitionBy("order_id", "batch_date").orderBy(col("ingestion_timestamp").desc())
        checked = (
            standardized.withColumn("row_number", row_number().over(window))
            .withColumn(
                "dq_reason",
                concat_ws(
                    "|",
                    when(col("order_id").isNull() | (col("order_id") == ""), "MISSING_ORDER_ID"),
                    when(col("customer_id").isNull() | (col("customer_id") == ""), "MISSING_CUSTOMER_ID"),
                    when(col("order_date").isNull(), "MISSING_ORDER_DATE"),
                    when(col("order_date") > col("batch_date"), "FUTURE_ORDER_DATE"),
                    when(col("order_status").isNull() | ~col("order_status").isin(*VALID_STATUSES), "INVALID_ORDER_STATUS"),
                    when(col("order_total").isNull() | (col("order_total") <= 0), "INVALID_ORDER_TOTAL"),
                    when(col("currency").isNull() | (col("currency") != "INR"), "INVALID_CURRENCY"),
                    when(col("row_number") > 1, "DUPLICATE_RECORD"),
                ),
            ).cache()
        )
        valid_orders = checked.filter(col("dq_reason") == "").withColumn("silver_updated_timestamp", current_timestamp()).withColumn("processed_by", lit(PROCESSED_BY)).select(*BUSINESS_COLUMNS, "batch_date", "silver_updated_timestamp", "processed_by")
        invalid_orders = checked.filter(col("dq_reason") != "").withColumn("quarantine_timestamp", current_timestamp()).withColumn("processed_by", lit(PROCESSED_BY)).select(*BUSINESS_COLUMNS, "batch_date", "ingestion_timestamp", "source_file", "source_system", "dq_reason", "quarantine_timestamp", "processed_by")
        bronze_count, valid_count, invalid_count = checked.count(), valid_orders.count(), invalid_orders.count()
        batch_dates = [row["batch_date"] for row in checked.select("batch_date").distinct().orderBy("batch_date").collect()]
        print(f"Bronze input row count: {bronze_count}", flush=True)
        print(f"Valid Silver row count: {valid_count}", flush=True)
        print(f"Quarantine row count: {invalid_count}", flush=True)
        print(f"Distinct batch dates processed: {batch_dates}", flush=True)
        print(f"Processed by: {PROCESSED_BY}", flush=True)
        batch_quarantine_path = f"{QUARANTINE_PATH}batch_date={batch_date}/"
        invalid_orders.write.mode("overwrite").parquet(batch_quarantine_path)
        delete_target_batch(client, batch_date)
        if valid_count:
            valid_orders.write.format("bigquery").option("writeMethod", "direct").mode("append").save(TARGET_TABLE)
        write_audit(client, batch_date, "INCREMENTAL_BATCH_COMPLETED")
        print(f"Successfully wrote Silver batch to: {TARGET_TABLE}", flush=True)
        print(f"Successfully wrote quarantine path: {batch_quarantine_path}", flush=True)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
