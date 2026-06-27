with positions as (
    select * from {{ ref('int_positions_enriched') }}
)

select
    agency,
    mode,
    route_id,
    route_name,
    report_date,
    report_hour,
    count(*) as report_count,
    sum(case when is_delayed then 1 else 0 end) as delayed_count,
    count(distinct vehicle_id) as active_vehicles,
    sum(case when is_delayed then 1 else 0 end) * 1.0 / nullif(count(*), 0) as delayed_rate
from positions
group by 1, 2, 3, 4, 5, 6
