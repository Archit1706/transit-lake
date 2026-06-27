with source as (
    select * from {{ source('silver', 'silver_vehicle_positions') }}
)

select
    {{ surrogate_key(['mode', 'vehicle_id', 'report_ts']) }} as position_sk,
    agency,
    mode,
    vehicle_id,
    route_id,
    headsign,
    lat,
    lon,
    heading,
    is_delayed,
    report_ts,
    cast(report_ts as date) as report_date,
    extract(hour from report_ts) as report_hour,
    _ingested_at
from source
where
    lat is not null
    and lon is not null
