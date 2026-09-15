"""Upload source datasets to GCS Landing for a batch date."""

import argparse
from datetime import datetime
from pathlib import Path
import sys


BUCKET_NAME = "gcp-ecommerce-de-dev-landing"
SOURCE_DIR = Path(__file__).resolve().parents[1] / "source_data"
SOURCE_FILES = (
    "customers.csv",
    "products.parquet",
    "orders.csv",
    "order_items.parquet",
    "payments.csv",
    "shipments.parquet",
    "inventory.csv",
    "returns.csv",
    "promotions.parquet",
    "customer_events.parquet",
)


def parse_batch_date(value):
    """Require a real calendar date written as YYYY-MM-DD."""
    try:
        parsed = datetime.strptime(value, "%Y-%m-%d")
    except ValueError as exc:
        raise argparse.ArgumentTypeError("batch_date must be YYYY-MM-DD") from exc
    if parsed.strftime("%Y-%m-%d") != value:
        raise argparse.ArgumentTypeError("batch_date must be YYYY-MM-DD")
    return value


def upload_source_batch(batch_date, batch_source_dir=None):
    """Upload one validated source batch and raise if any upload fails."""
    batch_date = parse_batch_date(batch_date)
    batch_source_dir = batch_source_dir or SOURCE_DIR / f"batch_date={batch_date}"
    # Check every expected file before starting any uploads.
    missing_files = [name for name in SOURCE_FILES if not (batch_source_dir / name).is_file()]
    if missing_files:
        missing_paths = ", ".join(str(batch_source_dir / name) for name in missing_files)
        raise FileNotFoundError(f"Missing source files: {missing_paths}")

    try:
        from google.cloud import storage
    except ImportError as exc:
        raise RuntimeError(
            "Install the GCS client: pip install google-cloud-storage"
        ) from exc

    # Uses Application Default Credentials from the local environment.
    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)
    for filename in SOURCE_FILES:
        local_path = batch_source_dir / filename
        dataset_name = local_path.stem
        destination = f"{dataset_name}/batch_date={batch_date}/{filename}"
        uri = f"gs://{BUCKET_NAME}/{destination}"
        print(f"Uploading {local_path} to {uri}", flush=True)
        bucket.blob(destination).upload_from_filename(str(local_path))
        print(f"Uploaded successfully: {uri}", flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("batch_date", type=parse_batch_date, help="Batch date (YYYY-MM-DD)")
    args = parser.parse_args()

    try:
        upload_source_batch(args.batch_date)
    except Exception as exc:
        print(f"Upload failed: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
