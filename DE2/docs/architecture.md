# Architecture

## Data flow

1. **Source** — `s3://ookla-open-data/parquet/performance/type={fixed|mobile}/year=YYYY/quarter={1..4}/`  
   Public S3 bucket, no auth. Each file is ~350 MB, ~6.3 M rows of tile-level
   quarterly aggregates (z16 Web Mercator tiles, ≈ 610 m on a side at the
   equator).
2. **Ingestion** — `ingestion/ookla_ingest.py`  
   Streams the S3 parquet to local disk, filters rows whose `tile_x / tile_y`
   fall inside the Malaysia bounding box (lon 99.5–119.5, lat 0.8–7.5),
   writes a smaller snappy parquet, uploads to
   `gs://my-speed-lake-{project}/ookla/type=.../year=.../quarter=.../data.parquet`.
3. **Orchestration** — `airflow/dags/my_speed_pipeline.py`  
   - `my_speed_backfill`: iterates a hand-picked list of (type, year, quarter) combos via dynamic task mapping.  
   - `my_speed_quarterly`: cron `0 3 15 2,5,8,11 *`, resolves the latest
     completed quarter from the run date, ingests fixed + mobile, refreshes
     the BigQuery external table, runs dbt.
4. **Warehouse** — `speed_raw.ookla_tiles_ext`  
   BigQuery external table over the GCS prefix, Hive partitioning auto-detected.
5. **Transform** — `dbt/` models:
   - `stg_ookla_tiles` (view): cast hive cols, kbps → Mbps, derive `quarter_start` date.
   - `int_tile_state` (view): priority-ranked bbox join against `my_state_bbox` seed to attach a state code.
   - `fct_tile_performance` (table, partitioned on `quarter_start` month, clustered by `connection_type, state_code`): tile-level fact.
   - `fct_state_quarterly`: state × quarter × type aggregates (tests-weighted mean).
   - `fct_national_quarterly`: small national time series for the trend chart.
6. **Visualisation** — `dashboard/app.py`  
   Streamlit + Plotly. Queries BigQuery directly with service-account creds.

## Partitioning & clustering rationale

- `fct_tile_performance` is partitioned by `quarter_start` (DATE, month
  granularity). Every dashboard query filters on quarter, so this prunes
  ~75 % of storage per query (with 4 quarters × 2 types).
- Clustering on `(connection_type, state_code)` matches the two most common
  `WHERE` filters in the dashboard (radio button + state selector).

## State attribution

Ookla ships no country/admin metadata. Rather than pull a full GIS shapefile
and do a spatial join, we use a **seed CSV of rough state bounding boxes**
(`dbt/seeds/my_state_bbox.csv`) with a `priority` column so overlapping boxes
resolve deterministically (e.g. KL bbox sits inside Selangor, but priority 1
beats priority 2). Accuracy is good enough for bar charts and maps at the
state level; a shapefile-based refinement is a straightforward future
improvement.

## Known simplifications

- No real shapefile join — see above.
- The backfill list is hard-coded rather than parameterised on CLI; it's
  trivially editable at the top of the DAG file.
- `avg_lat_ms` is averaged weighted by `tests` for national/state rollups,
  which isn't perfectly correct (harmonic mean would be better), but the
  difference is tiny at quarterly granularity.
