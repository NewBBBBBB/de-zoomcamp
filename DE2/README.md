# Malaysia Internet Speed — Data Engineering Capstone

End-to-end pipeline that pulls **Ookla Speedtest Open Data** (global fixed &
mobile broadband tiles), narrows it down to Malaysia, aggregates by state,
and serves a Streamlit dashboard.

```
Ookla S3 (public)         Airflow (LocalExecutor)
 parquet/performance/  ──▶  download + MY bbox filter
                           │
                           ▼
                        GCS data lake        ──▶  BigQuery external table
                        gs://…/ookla/                                  │
                         type=.../year=.../quarter=.../data.parquet    │
                                                                       ▼
                                                 dbt (staging → marts)
                                                 • stg_ookla_tiles (view)
                                                 • int_tile_state (view, bbox join)
                                                 • fct_tile_performance    (partitioned, clustered)
                                                 • fct_state_quarterly     (state × quarter × type)
                                                 • fct_national_quarterly  (time series)
                                                                       │
                                                                       ▼
                                                              Streamlit dashboard
                                                              (map + trend + ranking)
```

## Zoomcamp rubric coverage

| Area | How it's implemented |
|------|----------------------|
| Cloud + IaC | GCP (GCS + BigQuery + IAM SA) provisioned by `terraform/` |
| Workflow orchestration | Two Airflow DAGs (`my_speed_backfill`, `my_speed_quarterly`) |
| Data lake | GCS, partitioned by `type/year/quarter` (Hive-style) |
| Data warehouse | BigQuery — external table + dbt marts partitioned on `quarter_start`, clustered by `connection_type, state_code` |
| Transformations | dbt models with `ref`, tests, seeds, docs |
| Dashboard | Streamlit with 3 tiles (KPIs, national trend line, tile scatter map + state bar chart) |
| Reproducibility | Makefile, docker-compose, `.example` config files, this README |

## Prerequisites

- `gcloud` CLI, already logged in (`gcloud auth application-default login`)
- `terraform` ≥ 1.5
- Docker Desktop running
- Python 3.11+
- ≈ 1 GB free disk while downloading Ookla parquets (they are ~350 MB each)

## 1 — Provision infrastructure

```bash
cd terraform
cp terraform.tfvars.example terraform.tfvars   # edit project_id if needed
terraform init -upgrade
terraform apply -auto-approve
```

This creates the GCS lake bucket, two BQ datasets (`speed_raw`, `speed_marts`),
a service account, and a local key at `../credentials/sa-key.json`.

## 2 — Start Airflow

```bash
cd ../airflow
docker compose build
docker compose up -d
# UI: http://localhost:8080  (admin / admin)
```

The DAGs `my_speed_backfill` and `my_speed_quarterly` will be visible after a
few seconds. Trigger the backfill either from the UI or:

```bash
make trigger-backfill
```

The backfill downloads 10 parquet files (5 quarters × 2 types), writes them
to GCS, rebuilds the BigQuery external table, and runs `dbt build`.
Expected runtime: **5–8 minutes** on a typical laptop (most of it network I/O
pulling ~3.3 GB of global parquet and filtering it down).

## 3 — Launch the dashboard

```bash
cd ..
make dashboard-install
make dashboard
# Streamlit: http://localhost:8501
```

## Local pipeline (skip Airflow)

Useful for quickly iterating on dbt models:

```bash
make ingest-one TYPE=fixed YEAR=2025 Q=1
python3 scripts/refresh_ext_table.py
make dbt-build
make dashboard
```

## Cleanup

```bash
make airflow-nuke
make tf-destroy
```

## Data license

Data © Ookla 2019-present. Licensed under
[CC BY-NC-SA 4.0](https://creativecommons.org/licenses/by-nc-sa/4.0/).
