-- Worst congestion segments overall, with their slowest hour.
with seg_hour as (
    select * from {{ ref('int_congestion_segment_hour') }}
),

by_segment as (
    select
        segment_id,
        any_value(street) as street,
        any_value(from_street) as from_street,
        any_value(to_street) as to_street,
        sum(observation_count) as observations,
        avg(avg_speed_mph) as avg_speed_mph,
        avg(start_lat) as start_lat,
        avg(start_lon) as start_lon
    from seg_hour
    group by segment_id
),

slowest_hour as (
    select segment_id, report_hour as worst_hour
    from seg_hour
    qualify row_number() over (partition by segment_id order by avg_speed_mph asc) = 1
)

select
    b.segment_id,
    b.street,
    b.from_street,
    b.to_street,
    b.observations,
    b.avg_speed_mph,
    s.worst_hour,
    b.start_lat,
    b.start_lon,
    rank() over (order by b.avg_speed_mph asc) as congestion_rank
from by_segment b
left join slowest_hour s using (segment_id)
order by congestion_rank
