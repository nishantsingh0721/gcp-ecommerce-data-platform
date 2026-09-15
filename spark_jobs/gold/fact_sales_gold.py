"""Incrementally merge one Silver batch into the Gold fact_sales table."""

import sys
from datetime import datetime

from google.cloud import bigquery
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat_ws, current_timestamp, lit


PROJECT_ID = "gcp-ecommerce-de-dev"
ORDER_ITEMS_TABLE = f"{PROJECT_ID}.silver.order_items"
ORDERS_TABLE = f"{PROJECT_ID}.silver.orders"
CUSTOMERS_TABLE = f"{PROJECT_ID}.silver.customers"
PRODUCTS_TABLE = f"{PROJECT_ID}.silver.products"
TARGET_TABLE = f"{PROJECT_ID}.gold.fact_sales"
AUDIT_TABLE = f"{PROJECT_ID}.gold.pipeline_audit"
OBJECT_NAME = "fact_sales"
PROCESSED_BY = "fact_sales_gold"

FACT_COLUMNS = (
    "order_item_id",
    "order_id",
    "order_date",
    "order_status",
    "customer_id",
    "customer_name",
    "customer_city",
    "customer_state",
    "customer_status",
    "product_id",
    "product_name",
    "category",
    "brand",
    "quantity",
    "unit_price",
    "discount_amount",
    "line_amount",
    "shipping_city",
    "shipping_state",
    "source_batch_date",
    "gold_updated_timestamp",
    "processed_by",
)


def parse_batch_date():
    """Require exactly one valid YYYY-MM-DD command-line argument."""
    if len(sys.argv) != 2:
        raise ValueError("Usage: fact_sales_gold.py YYYY-MM-DD")
    try:
        return datetime.strptime(sys.argv[1], "%Y-%m-%d").date()
    except ValueError as exc:
        raise ValueError(
            "batch_date must be a valid date in YYYY-MM-DD format"
        ) from exc


def ensure_audit_table(client):
    """Create the Gold audit table when it is not present."""
    client.query(
        f"""
        CREATE TABLE IF NOT EXISTS `{AUDIT_TABLE}` (
            object_name STRING,
            batch_date DATE,
            status STRING,
            processed_at TIMESTAMP,
            message STRING
        )
        """
    ).result()


def batch_already_succeeded(client, batch_date):
    """Return whether this fact and batch already has a SUCCESS audit row."""
    config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("object_name", "STRING", OBJECT_NAME),
            bigquery.ScalarQueryParameter("batch_date", "DATE", batch_date),
        ]
    )
    rows = client.query(
        f"""
        SELECT 1
        FROM `{AUDIT_TABLE}`
        WHERE object_name = @object_name
          AND batch_date = @batch_date
          AND status = 'SUCCESS'
        LIMIT 1
        """,
        job_config=config,
    ).result()
    return next(iter(rows), None) is not None


def write_success_audit(client, batch_date):
    """Record success only after the Gold MERGE has completed."""
    config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("object_name", "STRING", OBJECT_NAME),
            bigquery.ScalarQueryParameter("batch_date", "DATE", batch_date),
            bigquery.ScalarQueryParameter(
                "message", "STRING", "INCREMENTAL_GOLD_MERGE_COMPLETED"
            ),
        ]
    )
    client.query(
        f"""
        INSERT INTO `{AUDIT_TABLE}` (
            object_name, batch_date, status, processed_at, message
        )
        VALUES (
            @object_name, @batch_date, 'SUCCESS', CURRENT_TIMESTAMP(), @message
        )
        """,
        job_config=config,
    ).result()


def read_silver_batch(spark, table_name, batch_date):
    """Read only the requested Silver batch with pushdown and safety filters."""
    return (
        spark.read.format("bigquery")
        .option("filter", f"batch_date = '{batch_date}'")
        .load(table_name)
        .filter(col("batch_date") == lit(batch_date))
    )


def create_target_if_needed(client, staging_table):
    """Create an empty target using the staging table's inferred schema."""
    client.query(
        f"""
        CREATE TABLE IF NOT EXISTS `{TARGET_TABLE}`
        PARTITION BY order_date
        CLUSTER BY customer_id, product_id
        AS
        SELECT *
        FROM `{staging_table}`
        WHERE FALSE
        """
    ).result()


def merge_staging_into_target(client, staging_table):
    """Upsert transactions using order_item_id as the sole business key."""
    update_columns = tuple(
        column_name for column_name in FACT_COLUMNS if column_name != "order_item_id"
    )
    update_clause = ",\n            ".join(
        f"T.{column_name} = S.{column_name}" for column_name in update_columns
    )
    insert_columns = ", ".join(FACT_COLUMNS)
    insert_values = ", ".join(f"S.{column_name}" for column_name in FACT_COLUMNS)

    client.query(
        f"""
        MERGE `{TARGET_TABLE}` AS T
        USING `{staging_table}` AS S
        ON T.order_item_id = S.order_item_id
        WHEN MATCHED
          AND S.source_batch_date >= T.source_batch_date
        THEN
          UPDATE SET
            {update_clause}
        WHEN NOT MATCHED THEN
          INSERT ({insert_columns})
          VALUES ({insert_values})
        """
    ).result()


def main():
    batch_date = parse_batch_date()
    staging_table = f"{PROJECT_ID}.gold.fact_sales_stage_{batch_date:%Y%m%d}"
    print(f"Processing Gold fact_sales for batch_date={batch_date}", flush=True)

    client = bigquery.Client(project=PROJECT_ID)
    ensure_audit_table(client)
    if batch_already_succeeded(client, batch_date):
        print(
            f"SKIPPED: fact_sales batch_date={batch_date} "
            "already processed successfully",
            flush=True,
        )
        return

    spark = SparkSession.builder.appName("fact-sales-gold").getOrCreate()
    try:
        spark.conf.set("spark.sql.session.timeZone", "UTC")
        print(f"Silver order-items source: {ORDER_ITEMS_TABLE}", flush=True)
        print(f"Silver orders source: {ORDERS_TABLE}", flush=True)
        print(f"Silver customers source: {CUSTOMERS_TABLE}", flush=True)
        print(f"Silver products source: {PRODUCTS_TABLE}", flush=True)
        print(f"Gold target table: {TARGET_TABLE}", flush=True)
        print(f"Gold staging table: {staging_table}", flush=True)

        order_items = read_silver_batch(
            spark, ORDER_ITEMS_TABLE, batch_date
        ).alias("oi")
        orders = read_silver_batch(spark, ORDERS_TABLE, batch_date).alias("o")
        customers = read_silver_batch(
            spark, CUSTOMERS_TABLE, batch_date
        ).alias("c")
        products = read_silver_batch(spark, PRODUCTS_TABLE, batch_date).alias("p")

        incoming_count = order_items.count()
        if incoming_count == 0:
            raise RuntimeError(
                f"No Silver order_items rows found for batch_date={batch_date}"
            )

        joined = (
            order_items.join(
                orders,
                col("oi.order_id") == col("o.order_id"),
                "left",
            )
            .join(
                customers,
                col("o.customer_id") == col("c.customer_id"),
                "left",
            )
            .join(
                products,
                col("oi.product_id") == col("p.product_id"),
                "left",
            )
        ).cache()

        missing_orders = joined.filter(col("o.order_id").isNull()).count()
        missing_customers = joined.filter(
            col("o.order_id").isNotNull() & col("c.customer_id").isNull()
        ).count()
        missing_products = joined.filter(col("p.product_id").isNull()).count()
        if missing_orders or missing_customers or missing_products:
            raise RuntimeError(
                "Gold fact_sales join integrity failed: "
                f"missing_orders={missing_orders}, "
                f"missing_customers={missing_customers}, "
                f"missing_products={missing_products}"
            )

        fact_sales = joined.select(
            col("oi.order_item_id").alias("order_item_id"),
            col("oi.order_id").alias("order_id"),
            col("o.order_date").alias("order_date"),
            col("o.order_status").alias("order_status"),
            col("o.customer_id").alias("customer_id"),
            concat_ws(" ", col("c.first_name"), col("c.last_name")).alias(
                "customer_name"
            ),
            col("c.city").alias("customer_city"),
            col("c.state").alias("customer_state"),
            col("c.customer_status").alias("customer_status"),
            col("oi.product_id").alias("product_id"),
            col("p.product_name").alias("product_name"),
            col("p.category").alias("category"),
            col("p.brand").alias("brand"),
            col("oi.quantity").alias("quantity"),
            col("oi.unit_price").alias("unit_price"),
            col("oi.discount_amount").alias("discount_amount"),
            col("oi.line_amount").alias("line_amount"),
            col("o.shipping_city").alias("shipping_city"),
            col("o.shipping_state").alias("shipping_state"),
            lit(batch_date).alias("source_batch_date"),
            current_timestamp().alias("gold_updated_timestamp"),
            lit(PROCESSED_BY).alias("processed_by"),
        ).cache()

        final_count = fact_sales.count()
        distinct_order_item_count = fact_sales.select(
            "order_item_id"
        ).distinct().count()
        print(f"Incoming order_items count: {incoming_count}", flush=True)
        print(f"Final joined fact count: {final_count}", flush=True)
        print(
            f"Distinct order_item_id count: {distinct_order_item_count}",
            flush=True,
        )
        if final_count != incoming_count or distinct_order_item_count != final_count:
            raise RuntimeError(
                "Gold fact_sales grain validation failed: "
                f"incoming_count={incoming_count}, final_count={final_count}, "
                f"distinct_order_item_id_count={distinct_order_item_count}"
            )

        staging_attempted = False
        merge_succeeded = False
        try:
            staging_attempted = True
            (
                fact_sales.write.format("bigquery")
                .option("writeMethod", "direct")
                .mode("overwrite")
                .save(staging_table)
            )
            create_target_if_needed(client, staging_table)
            merge_staging_into_target(client, staging_table)
            merge_succeeded = True
        finally:
            if staging_attempted:
                try:
                    client.delete_table(staging_table, not_found_ok=True)
                    print(f"Dropped staging table: {staging_table}", flush=True)
                except Exception as cleanup_error:
                    outcome = "after successful MERGE" if merge_succeeded else ""
                    print(
                        f"WARNING: staging cleanup failed {outcome}: {cleanup_error}",
                        flush=True,
                    )

        write_success_audit(client, batch_date)
        print(f"Successfully merged Gold table: {TARGET_TABLE}", flush=True)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
