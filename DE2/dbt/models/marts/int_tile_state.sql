-- Assign each tile to a single state by picking the lowest-priority matching bbox.
-- Priority is a manual disambiguation knob for overlapping boxes
-- (e.g. Kuala Lumpur bbox sits inside Selangor bbox).

{{ config(materialized='view') }}

with tiles as (
    select * from {{ ref('stg_ookla_tiles') }}
),
states as (
    select * from {{ ref('my_state_bbox') }}
),
joined as (
    select
        t.*,
        s.state_code,
        s.state_name,
        s.region,
        s.priority,
        row_number() over (
            partition by t.quadkey, t.connection_type, t.year, t.quarter
            order by s.priority, s.state_code
        ) as rn
    from tiles t
    left join states s
      on t.lon between s.lon_min and s.lon_max
     and t.lat between s.lat_min and s.lat_max
)
select
    quadkey, connection_type, year, quarter, quarter_start,
    lon, lat,
    avg_d_kbps, avg_u_kbps, avg_lat_ms,
    avg_d_mbps, avg_u_mbps,
    tests, devices,
    coalesce(state_code, 'UNK') as state_code,
    coalesce(state_name, 'Unknown') as state_name,
    coalesce(region, 'Unknown') as region
from joined
where rn = 1
