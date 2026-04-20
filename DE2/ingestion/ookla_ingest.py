"""
Download Ookla Open Data quarterly parquet for a given type/year/quarter,
pre-filter to Malaysia bounding box, and upload to GCS as partitioned parquet.

Ookla S3 layout:
  s3://ookla-open-data/parquet/performance/type={fixed|mobile}/
    year=YYYY/quarter={1..4}/YYYY-MM-DD_performance_{type}_tiles.parquet

GCS output layout:
  gs://{bucket}/ookla/type={type}/year={year}/quarter={quarter}/data.parquet
"""

from __future__ import annotations

import argparse
import logging
import tempfile
from pathlib import Path

import pyarrow.compute as pc
import pyarrow.parquet as pq
import requests
from google.cloud import storage

# Malaysia bounding box (generous, covers Peninsular + Sabah + Sarawak + some EEZ)
MY_BBOX = dict(lon_min=99.5, lon_max=119.5, lat_min=0.8, lat_max=7.5)

QUARTER_TO_MONTH = {1: "01", 2: "04", 3: "07", 4: "10"}

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger("ingest")


def ookla_url(data_type: str, year: int, quarter: int) -> str:
    month = QUARTER_TO_MONTH[quarter]
    fname = f"{year}-{month}-01_performance_{data_type}_tiles.parquet"
    return (
        "https://ookla-open-data.s3.amazonaws.com/parquet/performance/"
        f"type={data_type}/year={year}/quarter={quarter}/{fname}"
    )


def download(url: str, dest: Path) -> None:
    log.info("Downloading %s", url)
    with requests.get(url, stream=True, timeout=600) as r:
        r.raise_for_status()
        total = 0
        with open(dest, "wb") as f:
            for chunk in r.iter_content(chunk_size=8 * 1024 * 1024):
                f.write(chunk)
                total += len(chunk)
        log.info("  saved %.1f MB -> %s", total / 1024 / 1024, dest)


def filter_malaysia(src: Path, dst: Path) -> tuple[int, int]:
    """Filter by MY bbox. Return (rows_in, rows_out)."""
    log.info("Filtering to Malaysia bbox ...")
    table = pq.read_table(src)
    n_in = table.num_rows
    mask = (
        (pc.field("tile_x") >= MY_BBOX["lon_min"])
        & (pc.field("tile_x") <= MY_BBOX["lon_max"])
        & (pc.field("tile_y") >= MY_BBOX["lat_min"])
        & (pc.field("tile_y") <= MY_BBOX["lat_max"])
    )
    my = table.filter(mask)
    pq.write_table(my, dst, compression="snappy")
    log.info("  %d/%d rows kept (%.2f%%)", my.num_rows, n_in, 100 * my.num_rows / n_in)
    return n_in, my.num_rows


def upload_gcs(local: Path, bucket: str, blob_path: str) -> str:
    client = storage.Client()
    b = client.bucket(bucket)
    blob = b.blob(blob_path)
    log.info("Uploading to gs://%s/%s ...", bucket, blob_path)
    blob.upload_from_filename(str(local), timeout=600)
    uri = f"gs://{bucket}/{blob_path}"
    log.info("  done %s (%.1f MB)", uri, local.stat().st_size / 1024 / 1024)
    return uri


def run(
    data_type: str,
    year: int,
    quarter: int,
    bucket: str,
    workdir: Path | None = None,
) -> str:
    assert data_type in ("fixed", "mobile")
    assert 1 <= quarter <= 4

    workdir = workdir or Path(tempfile.mkdtemp(prefix="ookla-"))
    workdir.mkdir(parents=True, exist_ok=True)

    raw = workdir / f"global_{data_type}_{year}q{quarter}.parquet"
    filtered = workdir / f"my_{data_type}_{year}q{quarter}.parquet"

    download(ookla_url(data_type, year, quarter), raw)
    filter_malaysia(raw, filtered)

    blob_path = (
        f"ookla/type={data_type}/year={year}/quarter={quarter}/data.parquet"
    )
    uri = upload_gcs(filtered, bucket, blob_path)

    # cleanup big raw file, keep filtered for debugging if needed
    raw.unlink(missing_ok=True)
    return uri


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--type", choices=["fixed", "mobile"], required=True)
    p.add_argument("--year", type=int, required=True)
    p.add_argument("--quarter", type=int, required=True, choices=[1, 2, 3, 4])
    p.add_argument("--bucket", required=True)
    p.add_argument("--workdir", type=Path, default=None)
    args = p.parse_args()

    uri = run(args.type, args.year, args.quarter, args.bucket, args.workdir)
    print(uri)


if __name__ == "__main__":
    main()
