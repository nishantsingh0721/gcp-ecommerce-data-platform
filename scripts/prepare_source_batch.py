"""Generate, validate, and upload one source-data batch."""

import argparse
import sys
import tempfile
from pathlib import Path


REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
if str(REPOSITORY_ROOT) not in sys.path:
    sys.path.insert(0, str(REPOSITORY_ROOT))

from data_generator.generator import generate_source_batch, parse_batch_date
from scripts.upload_to_landing import upload_source_batch
from scripts.validate_source_data import validate


def prepare_source_batch(batch_date: str) -> str:
    """Prepare one batch in temporary worker storage and return its batch date."""
    batch_date = parse_batch_date(batch_date)

    with tempfile.TemporaryDirectory(prefix="ecommerce-source-") as temporary_dir:
        print(f"Generating source batch for batch_date={batch_date}", flush=True)
        batch_source_dir = generate_source_batch(
            batch_date,
            output_base_dir=Path(temporary_dir),
        )

        print(f"Validating source batch at {batch_source_dir}", flush=True)
        if not validate(batch_source_dir, batch_date):
            raise RuntimeError(f"Source validation failed for batch_date={batch_date}")

        print(f"Uploading validated batch_date={batch_date} to GCS Landing", flush=True)
        upload_source_batch(batch_date, batch_source_dir)

    print(f"Source batch preparation completed for batch_date={batch_date}", flush=True)
    return batch_date


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("batch_date", type=parse_batch_date, help="Batch date (YYYY-MM-DD)")
    args = parser.parse_args()
    prepare_source_batch(args.batch_date)
    return 0


if __name__ == "__main__":
    sys.exit(main())
