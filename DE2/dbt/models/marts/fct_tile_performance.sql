-- Tile-level fact. Partitioned by quarter_start (DAY) for cost-efficient
-- range queries; clustered by (connection_type, state_code) since those are
-- the most common filter dimensions in the dashboard.

{{ config(
    materialized='table',
    partition_by={'field': 'quarter_start', 'data_type': 'date', 'granularity': 'month'},
    cluster_by=['connection_type', 'state_code']
) }}

select
    quadkey,
    connection_type,
    year,
    quarter,
    quarter_start,
    state_code,
    state_name,
    region,
    lon,
    lat,
    avg_d_mbps,
    avg_u_mbps,
    avg_lat_ms,
    tests,
    devices
from {{ ref('int_tile_state') }}
