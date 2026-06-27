with stops as (
    select * from {{ ref('stg_cta__stops') }}
)

select
    {{ surrogate_key(['agency', 'stop_id']) }} as stop_sk,
    agency,
    stop_id,
    stop_code,
    stop_name,
    stop_lat,
    stop_lon,
    location_type,
    parent_station,
    wheelchair_boarding
from stops
