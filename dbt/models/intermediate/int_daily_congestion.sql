with congestion as (
    select * from {{ ref('stg_chicago__congestion') }}
)

select
    report_date,
    count(*) as observation_count,
    count(distinct segment_id) as active_segments,
    avg(speed_mph) as avg_speed_mph,
    -- congestion index: lower speed -> higher congestion (relative to a 30 mph free-flow proxy)
    avg(greatest(0, 1 - speed_mph / 30.0)) as congestion_index
from congestion
group by 1
