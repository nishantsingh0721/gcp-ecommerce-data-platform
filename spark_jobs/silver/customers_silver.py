"""Incrementally standardize and validate one Bronze customers batch."""

import sys
from datetime import datetime

from google.api_core.exceptions import NotFound
from google.cloud import bigquery

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    concat_ws,
    current_timestamp,
    lit,
    lower,
    row_number,
    trim,
    upper,
    when,
)
from pyspark.sql.window import Window


SOURCE_TABLE = "gcp-ecommerce-de-dev.bronze.customers"
TARGET_TABLE = "gcp-ecommerce-de-dev.silver.customers"
QUARANTINE_PATH = "gs://gcp-ecommerce-de-dev-quarantine/customers/"
AUDIT_TABLE = "gcp-ecommerce-de-dev.silver.pipeline_audit"
DATASET_NAME = "customers"
PROCESSED_BY = "customers_silver"

EMAIL_PATTERN = r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$"
VALID_CUSTOMER_STATUSES = ("ACTIVE", "INACTIVE")

SILVER_COLUMNS = (
    "customer_id",
    "first_name",
    "last_name",
    "email",
    "phone",
    "city",
    "state",
    "country",
    "signup_date",
    "customer_status",
    "batch_date",
    "silver_updated_timestamp",
    "processed_by",
)

QUARANTINE_COLUMNS = (
    "customer_id",
    "first_name",
    "last_name",
    "email",
    "phone",
    "city",
    "state",
    "country",
    "signup_date",
    "customer_status",
    "batch_date",
    "ingestion_timestamp",
    "source_file",
    "source_system",
    "dq_reason",
    "quarantine_timestamp",
    "processed_by",
)


def parse_batch_date():
    if len(sys.argv) != 2:
        raise ValueError("Usage: customers_silver.py YYYY-MM-DD")
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
    rows = client.query(sql, job_config=config).result()
    return next(iter(rows), None) is not None


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
    client.query(
        f"DELETE FROM `{TARGET_TABLE}` WHERE batch_date = @batch_date",
        job_config=query_config(batch_date),
    ).result()


def main():
    """Process one customers batch unless it has already succeeded."""
    batch_date = parse_batch_date()
    print(f"Processing Silver customers for batch_date={batch_date}", flush=True)
    client = bigquery.Client(project=TARGET_TABLE.split(".")[0])
    ensure_audit_table(client)
    if batch_exists(client, AUDIT_TABLE, batch_date, successful_only=True):
        print(f"SKIPPED: customers batch_date={batch_date} already processed successfully", flush=True)
        return
    if batch_exists(client, TARGET_TABLE, batch_date):
        write_audit(client, batch_date, "BOOTSTRAPPED_EXISTING_SILVER_BATCH")
        print(f"SKIPPED: existing customers Silver batch_date={batch_date} was bootstrapped", flush=True)
        return

    spark = SparkSession.builder.appName("customers-silver").getOrCreate()

    try:
        spark.conf.set("spark.sql.session.timeZone", "UTC")
        print(f"Bronze source table: {SOURCE_TABLE}", flush=True)
        print(f"Silver target table: {TARGET_TABLE}", flush=True)
        print(f"Quarantine path: {QUARANTINE_PATH}", flush=True)

        bronze_customers = (
            spark.read.format("bigquery")
            .option("filter", f"batch_date = '{batch_date}'")
            .load(SOURCE_TABLE)
            .filter(col("batch_date") == lit(batch_date))
        )
        if bronze_customers.limit(1).count() == 0:
            raise RuntimeError(f"No Bronze customers rows found for batch_date={batch_date}")

        standardized_customers = (
            bronze_customers
            .withColumn("customer_id", trim(col("customer_id")))
            .withColumn("first_name", trim(col("first_name")))
            .withColumn("last_name", trim(col("last_name")))
            .withColumn("email", lower(trim(col("email"))))
            .withColumn("phone", trim(col("phone")))
            .withColumn("city", trim(col("city")))
            .withColumn("state", trim(col("state")))
            .withColumn("country", trim(col("country")))
            .withColumn("customer_status", upper(trim(col("customer_status"))))
        )

        duplicate_window = Window.partitionBy(
            "customer_id", "batch_date"
        ).orderBy(col("ingestion_timestamp").desc())

        ranked_customers = standardized_customers.withColumn(
            "row_number", row_number().over(duplicate_window)
        )

        customers_with_dq = ranked_customers.withColumn(
            "dq_reason",
            concat_ws(
                "|",
                when(
                    col("customer_id").isNull() | (col("customer_id") == ""),
                    "MISSING_CUSTOMER_ID",
                ),
                when(
                    col("email").isNull()
                    | (col("email") == "")
                    | ~col("email").rlike(EMAIL_PATTERN),
                    "INVALID_EMAIL",
                ),
                when(
                    col("customer_status").isNull()
                    | ~col("customer_status").isin(*VALID_CUSTOMER_STATUSES),
                    "INVALID_CUSTOMER_STATUS",
                ),
                when(col("signup_date").isNull(), "MISSING_SIGNUP_DATE"),
                when(col("signup_date") > col("batch_date"), "FUTURE_SIGNUP_DATE"),
                when(col("row_number") > 1, "DUPLICATE_RECORD"),
            ),
        ).cache()

        valid_customers = (
            customers_with_dq
            .filter(col("dq_reason") == "")
            .withColumn("silver_updated_timestamp", current_timestamp())
            .withColumn("processed_by", lit(PROCESSED_BY))
            .select(*SILVER_COLUMNS)
        )
        invalid_customers = (
            customers_with_dq
            .filter(col("dq_reason") != "")
            .withColumn("quarantine_timestamp", current_timestamp())
            .withColumn("processed_by", lit(PROCESSED_BY))
            .select(*QUARANTINE_COLUMNS)
        )

        bronze_count = customers_with_dq.count()
        valid_count = valid_customers.count()
        quarantine_count = invalid_customers.count()
        batch_dates = [
            row["batch_date"]
            for row in customers_with_dq.select("batch_date").distinct().orderBy(
                "batch_date"
            ).collect()
        ]

        print(f"Bronze input row count: {bronze_count}", flush=True)
        print(f"Valid Silver row count: {valid_count}", flush=True)
        print(f"Quarantine row count: {quarantine_count}", flush=True)
        print(f"Distinct batch dates being processed: {batch_dates}", flush=True)

        batch_quarantine_path = f"{QUARANTINE_PATH}batch_date={batch_date}/"
        invalid_customers.write.mode("overwrite").parquet(batch_quarantine_path)

        delete_target_batch(client, batch_date)
        if valid_count:
            valid_customers.write.format("bigquery").option("writeMethod", "direct").mode("append").save(TARGET_TABLE)
        write_audit(client, batch_date, "INCREMENTAL_BATCH_COMPLETED")

        print(f"Successfully wrote Silver batch to: {TARGET_TABLE}", flush=True)
        print(f"Successfully wrote quarantine path: {batch_quarantine_path}", flush=True)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
