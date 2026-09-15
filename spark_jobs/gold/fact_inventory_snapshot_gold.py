"""Incrementally merge one Silver batch into Gold fact_inventory_snapshot."""

import sys
from datetime import datetime

from google.cloud import bigquery
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, lit

PROJECT_ID = "gcp-ecommerce-de-dev"
SOURCE_TABLE = f"{PROJECT_ID}.silver.inventory"
TARGET_TABLE = f"{PROJECT_ID}.gold.fact_inventory_snapshot"
AUDIT_TABLE = f"{PROJECT_ID}.gold.pipeline_audit"
OBJECT_NAME = "fact_inventory_snapshot"
PROCESSED_BY = "fact_inventory_snapshot_gold"
BUSINESS_COLUMNS = ("inventory_id", "product_id", "warehouse_id", "warehouse_city", "stock_quantity", "reorder_level", "last_updated")
MERGE_KEYS = ("product_id", "warehouse_id", "source_batch_date")
OUTPUT_COLUMNS = BUSINESS_COLUMNS + (
    "source_batch_date", "gold_updated_timestamp", "processed_by",
)


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
        bigquery.ScalarQueryParameter("message", "STRING", "INCREMENTAL_GOLD_SNAPSHOT_COMPLETED"),
    ])
    client.query(
        f"""INSERT INTO `{AUDIT_TABLE}`
        (object_name, batch_date, status, processed_at, message)
        VALUES (@object_name, @batch_date, 'SUCCESS', CURRENT_TIMESTAMP(), @message)""",
        job_config=config,
    ).result()


def create_target_if_needed(client, staging_table):
    client.query(f"""CREATE TABLE IF NOT EXISTS `{TARGET_TABLE}`
        PARTITION BY source_batch_date\n        CLUSTER BY product_id, warehouse_id
        AS SELECT * FROM `{staging_table}` WHERE FALSE""").result()


def merge_staging(client, staging_table):
    match_condition = " AND ".join(f"T.{key} = S.{key}" for key in MERGE_KEYS)
    update_columns = tuple(column for column in OUTPUT_COLUMNS if column not in MERGE_KEYS)
    update_clause = ", ".join(f"T.{column} = S.{column}" for column in update_columns)
    columns = ", ".join(OUTPUT_COLUMNS)
    values = ", ".join(f"S.{column}" for column in OUTPUT_COLUMNS)
    client.query(f"""MERGE `{TARGET_TABLE}` T USING `{staging_table}` S
        ON {match_condition}
        WHEN MATCHED AND S.source_batch_date >= T.source_batch_date
          THEN UPDATE SET {update_clause}
        WHEN NOT MATCHED THEN INSERT ({columns}) VALUES ({values})""").result()


def main():
    batch_date = parse_batch_date()
    staging_table = f"{PROJECT_ID}.gold.fact_inventory_snapshot_stage_{batch_date:%Y%m%d}"
    print(f"Processing Gold {OBJECT_NAME} for batch_date={batch_date}", flush=True)
    client = bigquery.Client(project=PROJECT_ID)
    ensure_audit_table(client)
    if batch_already_succeeded(client, batch_date):
        print(f"SKIPPED: {OBJECT_NAME} batch_date={batch_date} already processed successfully", flush=True)
        return

    spark = SparkSession.builder.appName("fact-inventory-snapshot-gold").getOrCreate()
    try:
        spark.conf.set("spark.sql.session.timeZone", "UTC")
        source = (
            spark.read.format("bigquery")
            .option("filter", f"batch_date = '{batch_date}'")
            .load(SOURCE_TABLE)
            .filter(col("batch_date") == lit(batch_date))
        )
        source_count = source.count()
        if source_count == 0:
            raise RuntimeError(f"No Silver inventory rows found for batch_date={batch_date}")

        output = source.select(
            *[col(column) for column in BUSINESS_COLUMNS],
            lit(batch_date).alias("source_batch_date"),
            current_timestamp().alias("gold_updated_timestamp"),
            lit(PROCESSED_BY).alias("processed_by"),
        ).cache()
        output_count = output.count()
        distinct_count = output.select(*MERGE_KEYS).distinct().count()
        print(f"Source row count: {source_count}", flush=True)
        print(f"Output row count: {output_count}", flush=True)
        print(f"Distinct business grain count: {distinct_count}", flush=True)
        if output_count != source_count or distinct_count != output_count:
            raise RuntimeError(
                f"{OBJECT_NAME} grain validation failed: source={source_count}, "
                f"output={output_count}, distinct={distinct_count}"
            )

        stage_attempted = False
        merge_succeeded = False
        try:
            stage_attempted = True
            output.write.format("bigquery").option("writeMethod", "direct").mode("overwrite").save(staging_table)
            create_target_if_needed(client, staging_table)
            merge_staging(client, staging_table)
            merge_succeeded = True
        finally:
            if stage_attempted:
                try:
                    client.delete_table(staging_table, not_found_ok=True)
                    print(f"Dropped staging table: {staging_table}", flush=True)
                except Exception as cleanup_error:
                    outcome = "after successful MERGE" if merge_succeeded else ""
                    print(f"WARNING: staging cleanup failed {outcome}: {cleanup_error}", flush=True)

        write_success_audit(client, batch_date)
        print(f"Successfully merged Gold table: {TARGET_TABLE}", flush=True)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
