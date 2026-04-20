-- National time series (× connection_type). Tiny table; used for trend charts.

{{ config(materialized='table') }}

select
    connection_type,
    year,
    quarter,
    quarter_start,
    safe_divide(sum(avg_d_mbps * tests), sum(tests)) as avg_d_mbps,
    safe_divide(sum(avg_u_mbps * tests), sum(tests)) as avg_u_mbps,
    safe_divide(sum(avg_lat_ms * tests), sum(tests)) as avg_lat_ms,
    sum(tests)   as total_tests,
    sum(devices) as total_devices,
    count(distinct state_code) as states_covered
from {{ ref('fct_tile_performance') }}
where state_code != 'UNK'
group by 1,2,3,4
order by 4, 1
