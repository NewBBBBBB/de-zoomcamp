-- State × quarter × connection_type aggregate. Used by the dashboard map.
-- tests-weighted mean so busy cities dominate appropriately.

{{ config(
    materialized='table',
    partition_by={'field': 'quarter_start', 'data_type': 'date', 'granularity': 'month'},
    cluster_by=['connection_type', 'state_code']
) }}

select
    state_code,
    state_name,
    region,
    connection_type,
    year,
    quarter,
    quarter_start,
    safe_divide(sum(avg_d_mbps * tests), sum(tests)) as avg_d_mbps,
    safe_divide(sum(avg_u_mbps * tests), sum(tests)) as avg_u_mbps,
    safe_divide(sum(avg_lat_ms * tests), sum(tests)) as avg_lat_ms,
    sum(tests)   as total_tests,
    sum(devices) as total_devices,
    count(*)     as tile_count
from {{ ref('fct_tile_performance') }}
group by 1,2,3,4,5,6,7
