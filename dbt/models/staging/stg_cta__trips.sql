with source as (
    select * from {{ source('silver', 'silver_trips') }}
)

select
    agency,
    trip_id,
    route_id,
    service_id,
    direction_id,
    direction_name,
    block_id,
    shape_id,
    schd_trip_id,
    wheelchair_accessible
from source
