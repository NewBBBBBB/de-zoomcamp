"""
Malaysia Internet Speed — Ookla Open Data pipeline.

Two DAGs:
  - `my_speed_backfill`    : manual, iterates (type, year, quarter) combos to load.
  - `my_speed_quarterly`   : scheduled at Q+1 month 15 to fetch latest quarter.

Each DAG does:
  1. ingest_ookla_to_gcs   : Python call to ingestion.ookla_ingest.run(...)
  2. refresh_external_table: create/refresh BQ external table over gs://.../ookla/*
  3. dbt_build             : run dbt deps + build
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.decorators import task
from airflow.operators.bash import BashOperator

GCP_PROJECT = os.environ["GCP_PROJECT"]
GCS_BUCKET = os.environ["GCS_BUCKET"]
BQ_RAW = os.environ["BQ_RAW"]
BQ_LOCATION = os.environ.get("BQ_LOCATION", "US")

DEFAULT_ARGS = {
    "owner": "de2",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

# ---------- Backfill: hand-picked set of quarters we want in the lake ----------
# Keep this modest (each file is ~350 MB to download); edit freely and reparse.
BACKFILL_COMBOS = [
    # Fixed broadband: full 2023-2025 Q1 (9 quarters)
    ("fixed", 2023, 1),
    ("fixed", 2023, 2),
    ("fixed", 2023, 3),
    ("fixed", 2023, 4),
    ("fixed", 2024, 1),
    ("fixed", 2024, 2),
    ("fixed", 2024, 3),
    ("fixed", 2024, 4),
    ("fixed", 2025, 1),
    # Mobile: sampled (files are larger); still 6 points = enough trend
    ("mobile", 2023, 1),
    ("mobile", 2023, 3),
    ("mobile", 2024, 1),
    ("mobile", 2024, 2),
    ("mobile", 2024, 3),
    ("mobile", 2024, 4),
    ("mobile", 2025, 1),
]


def _ingest(data_type: str, year: int, quarter: int) -> str:
    """Call ingestion module; keep light-weight so Airflow worker handles it."""
    from ookla_ingest import run as run_ingest  # /opt/airflow/ingestion on PYTHONPATH

    return run_ingest(
        data_type=data_type,
        year=year,
        quarter=quarter,
        bucket=GCS_BUCKET,
        workdir=Path("/tmp/ookla"),
    )


def _refresh_external_table() -> None:
    """(Re)create BQ external table over the whole lake prefix."""
    from google.cloud import bigquery

    client = bigquery.Client(project=GCP_PROJECT, location=BQ_LOCATION)
    table_id = f"{GCP_PROJECT}.{BQ_RAW}.ookla_tiles_ext"

    ec = bigquery.ExternalConfig("PARQUET")
    ec.source_uris = [f"gs://{GCS_BUCKET}/ookla/*.parquet"]
    ec.autodetect = True
    hpo = bigquery.HivePartitioningOptions()
    hpo.mode = "AUTO"
    hpo.source_uri_prefix = f"gs://{GCS_BUCKET}/ookla/"
    ec.hive_partitioning = hpo

    table = bigquery.Table(table_id)
    table.external_data_configuration = ec

    # delete then create (idempotent)
    client.delete_table(table_id, not_found_ok=True)
    client.create_table(table)
    print(f"Created external table {table_id}")


# ---------- DAG: Backfill ----------
with DAG(
    dag_id="my_speed_backfill",
    description="One-shot: download many quarters of Ookla Open Data for Malaysia.",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    max_active_tasks=2,
    tags=["ookla", "malaysia"],
) as backfill_dag:

    @task
    def make_combos() -> list[dict]:
        return [
            {"data_type": t, "year": y, "quarter": q}
            for (t, y, q) in BACKFILL_COMBOS
        ]

    @task(max_active_tis_per_dag=2)
    def ingest_one(combo: dict) -> str:
        return _ingest(combo["data_type"], combo["year"], combo["quarter"])

    @task
    def refresh_external_table() -> None:
        _refresh_external_table()

    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=(
            "cd /opt/airflow/dbt && "
            "dbt deps --profiles-dir /opt/airflow/dbt && "
            "dbt build --profiles-dir /opt/airflow/dbt --target prod"
        ),
    )

    uris = ingest_one.expand(combo=make_combos())
    refresh = refresh_external_table()
    uris >> refresh >> dbt_build


# ---------- DAG: Quarterly refresh ----------
with DAG(
    dag_id="my_speed_quarterly",
    description="Quarterly: fetch most-recent Ookla quarter and rebuild marts.",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2026, 2, 15),
    # On 15th of Feb/May/Aug/Nov — Ookla publishes prior quarter ~mid-month.
    schedule="0 3 15 2,5,8,11 *",
    catchup=False,
    tags=["ookla", "malaysia"],
) as quarterly_dag:

    @task
    def resolve_latest(logical_date=None) -> list[dict]:
        """Derive the most recent completed Ookla quarter from run date."""
        d = logical_date or datetime.utcnow()
        month = d.month
        year = d.year
        # If run in Feb -> want prior year Q4; May -> Q1; Aug -> Q2; Nov -> Q3
        mapping = {2: (year - 1, 4), 5: (year, 1), 8: (year, 2), 11: (year, 3)}
        y, q = mapping.get(month, (year - 1, 4))
        return [
            {"data_type": "fixed", "year": y, "quarter": q},
            {"data_type": "mobile", "year": y, "quarter": q},
        ]

    @task
    def ingest_one_q(combo: dict) -> str:
        return _ingest(combo["data_type"], combo["year"], combo["quarter"])

    @task
    def refresh_external_table_q() -> None:
        _refresh_external_table()

    dbt_build_q = BashOperator(
        task_id="dbt_build",
        bash_command=(
            "cd /opt/airflow/dbt && "
            "dbt deps --profiles-dir /opt/airflow/dbt && "
            "dbt build --profiles-dir /opt/airflow/dbt --target prod"
        ),
    )

    combos = resolve_latest()
    uris_q = ingest_one_q.expand(combo=combos)
    refresh_q = refresh_external_table_q()
    uris_q >> refresh_q >> dbt_build_q
