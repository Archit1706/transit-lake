with source as (
    select * from {{ source('silver', 'silver_congestion') }}
)

select
    segment_id,
    report_ts,
    cast(report_ts as date) as report_date,
    hour as report_hour,
    day_of_week,
    month,
    speed_mph,
    street,
    direction,
    from_street,
    to_street,
    length_mi,
    bus_count,
    message_count,
    start_lat,
    start_lon,
    end_lat,
    end_lon
from source
