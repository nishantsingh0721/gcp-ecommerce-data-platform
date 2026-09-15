# gcp-ecommerce-data-platform
GCP data engineering project with synthetic batch data, GCS Landing storage,
Dataproc / PySpark processing, and Bronze, Silver, and Gold datasets in BigQuery.

## Target architecture

```text
Synthetic Data Generator
    ↓
Local batch: source_data/batch_date=YYYY-MM-DD/
    ↓
Validation
    ↓
GCS Landing
    ↓
Dataproc / PySpark
    ↓
BigQuery Bronze
    ↓
BigQuery Silver
    ↓
BigQuery Gold
```

PySpark jobs on Dataproc will perform each medallion transition. GCS is the
Landing/raw source storage layer; the processing layers are BigQuery datasets.

- GCP project: `gcp-ecommerce-de-dev`
- Region / BigQuery dataset location: `asia-south1`
- Landing bucket: `gcp-ecommerce-de-dev-landing`
- Target BigQuery datasets: `gcp-ecommerce-de-dev.bronze`,
  `gcp-ecommerce-de-dev.silver`, and `gcp-ecommerce-de-dev.gold`
- Later orchestration: Cloud Composer / Airflow
- Later infrastructure management: Terraform

These are architecture targets, not a declaration that BigQuery tables or
processing jobs have been created. Existing cloud resources, including any
previously created Bronze GCS bucket, may remain in GCP without serving as a
medallion processing layer.

## Existing batch workflow

Run from the repository root with the project's Python environment activated:

```bash
python data_generator/generator.py --batch-date 2026-09-09
python scripts/validate_source_data.py 2026-09-09
python scripts/upload_to_landing.py 2026-09-09
```

Run the upload after validation passes. Uploading requires Google Cloud
credentials with access to the Landing bucket. Generation, validation, and
upload remain separate responsibilities.

Each local batch contains the existing 10 files:

```text
source_data/batch_date=2026-09-09/
├── customers.csv
├── products.parquet
├── orders.csv
├── order_items.parquet
├── payments.csv
├── shipments.parquet
├── inventory.csv
├── returns.csv
├── promotions.parquet
└── customer_events.parquet
```

Landing objects retain this structure:

```text
gs://gcp-ecommerce-de-dev-landing/<dataset>/batch_date=<batch_date>/<filename>
```

For example: `gs://gcp-ecommerce-de-dev-landing/customers/batch_date=2026-09-09/customers.csv`.
Each batch continues to use the existing generation behavior; true incremental
record generation is future work.

## Repository organization

```text
gcp-ecommerce-data-platform/
├── data_generator/             # Existing synthetic generation and schemas
├── source_data/                # Generated local batches (ignored by Git)
├── scripts/                    # Generation helpers, validation, Landing upload
├── spark_jobs/
│   ├── __init__.py
│   ├── hello_dataproc.py       # Existing Dataproc smoke test
│   ├── bronze/
│   │   ├── __init__.py
│   │   └── ingest.py           # Existing empty placeholder
│   ├── silver/
│   │   ├── __init__.py
│   │   └── transform.py        # Existing empty placeholder
│   ├── gold/
│   │   ├── __init__.py
│   │   ├── build_facts.py      # Existing empty placeholder
│   │   ├── build_dimensions.py # Existing empty placeholder
│   │   └── build_aggregates.py # Existing empty placeholder
│   └── common/                # Existing shared-helper placeholders
├── config/                    # Existing configuration placeholders
├── dq/                        # Reserved for future data-quality implementation
├── dags/                      # Reserved for future Composer / Airflow DAGs
├── sql/                       # Existing SQL placeholders; table design is future work
├── terraform/                 # Reserved for later infrastructure management
├── tests/                     # Existing test scaffolding
├── bronze/                    # Legacy empty placeholder; outside active data flow
├── silver/                    # Legacy empty placeholder; outside active data flow
├── gold/                      # Legacy empty placeholder; outside active data flow
├── dev/                       # Existing environment placeholder
└── prod/                      # Existing environment placeholder
```

The processing directories have these responsibilities:

- `spark_jobs/bronze/`: read source files from GCS Landing and load BigQuery
  Bronze tables. The future customers job will be
  `spark_jobs/bronze/customers_bronze.py`; it is not implemented yet.
- `spark_jobs/silver/`: read BigQuery Bronze tables, apply cleaning,
  standardization, deduplication, and business/data-quality transformations,
  then write BigQuery Silver tables.
- `spark_jobs/gold/`: read trusted BigQuery Silver data and build analytics-ready
  BigQuery Gold fact, dimension, and aggregate tables.

The medallion jobs, BigQuery table definitions, orchestration, infrastructure
resources, and data-quality framework remain future work. The configuration
files are currently empty placeholders and define no GCS medallion buckets.
