-- Segment x hour average speed — the source for a congestion heatmap.
with seg_hour as (
    select * from {{ ref('int_congestion_segment_hour') }}
)

select
    segment_id,
    street,
    report_hour,
    sum(observation_count) as observations,
    avg(avg_speed_mph) as avg_speed_mph,
    min(min_speed_mph) as min_speed_mph
from seg_hour
group by 1, 2, 3
