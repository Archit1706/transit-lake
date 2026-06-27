-- City-wide average road speed by hour of day.
with seg_hour as (
    select * from {{ ref('int_congestion_segment_hour') }}
)

select
    report_hour,
    count(distinct segment_id) as segments,
    sum(observation_count) as observations,
    avg(avg_speed_mph) as avg_speed_mph
from seg_hour
group by report_hour
order by report_hour
