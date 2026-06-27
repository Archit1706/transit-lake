with source as (
    select * from {{ source('silver', 'silver_stops') }}
)

select
    agency,
    stop_id,
    stop_code,
    stop_name,
    stop_lat,
    stop_lon,
    location_type,
    parent_station,
    wheelchair_boarding
from source
where stop_lat is not null
  and stop_lon is not null
