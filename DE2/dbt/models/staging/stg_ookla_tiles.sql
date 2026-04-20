-- Normalise Ookla external table:
--   * cast hive partition columns
--   * kbps -> Mbps
--   * attach ISO quarter-start date for time-series plots

with src as (
    select * from {{ source('speed_raw', 'ookla_tiles_ext') }}
)

select
    quadkey,
    cast(type as string)                          as connection_type,   -- fixed / mobile
    cast(year as int64)                           as year,
    cast(quarter as int64)                        as quarter,
    date(cast(year as int64),
         (cast(quarter as int64) - 1) * 3 + 1,
         1)                                       as quarter_start,
    tile_x                                        as lon,
    tile_y                                        as lat,
    avg_d_kbps                                    as avg_d_kbps,
    avg_u_kbps                                    as avg_u_kbps,
    avg_lat_ms                                    as avg_lat_ms,
    avg_d_kbps / 1000.0                           as avg_d_mbps,
    avg_u_kbps / 1000.0                           as avg_u_mbps,
    tests,
    devices
from src
-- Defensive: the GCS lake is already Malaysia-only, but keep the bbox
-- so the model is correct even if someone uploads global data.
where tile_x between 99.5 and 119.5
  and tile_y between 0.8  and 7.5
