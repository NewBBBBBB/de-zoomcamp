"""Standalone helper to (re)create the BQ external table over the GCS lake.

Same logic as the Airflow task; useful for running outside Docker.
"""

from __future__ import annotations

import os

from google.cloud import bigquery


def main() -> None:
    project = os.environ.get("GCP_PROJECT", "iron-figure-312018")
    bucket = os.environ.get("GCS_BUCKET", f"my-speed-lake-{project}")
    raw = os.environ.get("BQ_RAW", "speed_raw")
    location = os.environ.get("BQ_LOCATION", "US")

    client = bigquery.Client(project=project, location=location)
    table_id = f"{project}.{raw}.ookla_tiles_ext"

    ec = bigquery.ExternalConfig("PARQUET")
    ec.source_uris = [f"gs://{bucket}/ookla/*.parquet"]
    ec.autodetect = True
    hpo = bigquery.HivePartitioningOptions()
    hpo.mode = "AUTO"
    hpo.source_uri_prefix = f"gs://{bucket}/ookla/"
    ec.hive_partitioning = hpo

    table = bigquery.Table(table_id)
    table.external_data_configuration = ec

    client.delete_table(table_id, not_found_ok=True)
    client.create_table(table)
    print(f"Created external table {table_id}")


if __name__ == "__main__":
    main()
