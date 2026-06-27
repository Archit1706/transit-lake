with source as (
    select * from {{ source('silver', 'silver_stop_times') }}
)

select
    agency,
    trip_id,
    stop_id,
    stop_sequence,
    arrival_time,
    departure_time,
    arrival_sec,
    -- service-day hour the trip is scheduled to reach this stop (handles 24h+)
    (arrival_sec // 3600) % 24 as scheduled_hour,
    stop_headsign,
    pickup_type,
    shape_dist_traveled
from source
