with congestion as (
    select * from {{ ref('stg_chicago__congestion') }}
)

select
    segment_id,
    street,
    direction,
    from_street,
    to_street,
    report_date,
    report_hour,
    day_of_week,
    count(*) as observation_count,
    avg(speed_mph) as avg_speed_mph,
    min(speed_mph) as min_speed_mph,
    avg(length_mi) as length_mi,
    avg(start_lat) as start_lat,
    avg(start_lon) as start_lon
from congestion
group by 1, 2, 3, 4, 5, 6, 7, 8
