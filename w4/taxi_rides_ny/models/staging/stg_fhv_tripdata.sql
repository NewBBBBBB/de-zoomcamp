select
    dispatching_base_num,

    cast(pulocationid as integer) as pickup_location_id,
    cast(dolocationid as integer) as dropoff_location_id,

    cast(pickup_datetime as timestamp) as pickup_datetime,
    cast(dropoff_datetime as timestamp) as dropoff_datetime,

    'FHV' as service_type

from {{ source('nytaxi', 'fhv_tripdata') }}
where dispatching_base_num is not null