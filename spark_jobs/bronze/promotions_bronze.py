"""Load a promotions batch from GCS Landing into BigQuery Bronze."""

import argparse
from datetime import date

from google.api_core.exceptions import NotFound
from google.cloud import bigquery

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, input_file_name, lit
from pyspark.sql.types import (
    DateType,
    DecimalType,
    StringType,
    StructField,
    StructType,
)


TARGET_TABLE = "gcp-ecommerce-de-dev.bronze.promotions"

# Column order matches PROMOTION_COLUMNS in data_generator/schemas.py.
# Expected Parquet types match the Arrow schema in the generator.
# Spark read nullability is not enforced as a Bronze business rule.
PROMOTION_SCHEMA = StructType([
    StructField("promotion_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("promotion_name", StringType(), True),
    StructField("discount_type", StringType(), True),
    StructField("discount_value", DecimalType(10, 2), True),
    StructField("start_date", DateType(), True),
    StructField("end_date", DateType(), True),
    StructField("promotion_status", StringType(), True),
])


def parse_batch_date(value):
    """Require a real calendar date in exactly YYYY-MM-DD format."""
    try:
        parsed = date.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            "batch_date must be a valid date in YYYY-MM-DD format"
        ) from exc
    if parsed.isoformat() != value:
        raise argparse.ArgumentTypeError(
            "batch_date must be a valid date in YYYY-MM-DD format"
        )
    return value


def delete_current_batch(batch_date):
    """Remove only this batch so a successful rerun replaces its previous rows."""
    with bigquery.Client(project=TARGET_TABLE.split(".")[0]) as client:
        try:
            client.get_table(TARGET_TABLE)
        except NotFound:
            # The append below can create a missing table in the existing dataset.
            print(f"Target table {TARGET_TABLE} does not exist; skipping DELETE", flush=True)
            return

        # The table identifier is a code constant; the date is a query parameter.
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("batch_date", "DATE", date.fromisoformat(batch_date))
            ]
        )
        print(f"Deleting batch_date={batch_date} from {TARGET_TABLE}", flush=True)
        client.query(
            f"DELETE FROM `{TARGET_TABLE}` WHERE batch_date = @batch_date",
            job_config=job_config,
            location="asia-south1",
        ).result()  # Wait for DELETE to finish before Spark appends any rows.
        print(f"Deleted existing batch_date={batch_date} rows", flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "batch_date", type=parse_batch_date,
        help="Batch date (YYYY-MM-DD)",
    )
    args = parser.parse_args()
    source_path = (
        "gs://gcp-ecommerce-de-dev-landing/promotions/"
        f"batch_date={args.batch_date}/promotions.parquet"
    )

    spark = SparkSession.builder.appName("promotions-bronze").getOrCreate()
    try:
        spark.conf.set("spark.sql.session.timeZone", "UTC")
        print(f"Source path: {source_path}", flush=True)
        print(f"Target table: {TARGET_TABLE}", flush=True)

        promotions = spark.read.parquet(source_path)
        promotions.printSchema()

        # Verify structure only; preserve the Parquet values and inferred types.
        expected_fields = [(field.name, field.dataType) for field in PROMOTION_SCHEMA]
        actual_fields = [(field.name, field.dataType) for field in promotions.schema]
        if actual_fields != expected_fields:
            raise ValueError(
                f"Source schema mismatch: expected {PROMOTION_SCHEMA.simpleString()}, "
                f"got {promotions.schema.simpleString()}"
            )

        bronze_promotions = (
            promotions
            .withColumn("batch_date", lit(args.batch_date).cast(DateType()))
            .withColumn("ingestion_timestamp", current_timestamp())
            .withColumn("source_file", input_file_name())
            .withColumn("source_system", lit("ecommerce_source"))
        )

        print(f"Input row count: {promotions.count()}", flush=True)
        print(f"Output row count (DataFrame): {bronze_promotions.count()}", flush=True)

        # Delete only this batch, then append: successful reruns do not duplicate rows.
        # Other batch_date rows are preserved. Do not run the same table/batch concurrently.
        # DELETE and append are separate operations; rerun this batch if append fails.
        delete_current_batch(args.batch_date)
        (
            bronze_promotions.write
            .format("bigquery")
            .option("writeMethod", "direct")
            .mode("append")
            .save(TARGET_TABLE)
        )
        print(f"Successfully loaded promotions into {TARGET_TABLE}", flush=True)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
