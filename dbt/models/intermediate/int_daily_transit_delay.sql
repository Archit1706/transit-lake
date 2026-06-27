with positions as (
    select * from {{ ref('int_positions_enriched') }}
)

select
    report_date,
    mode,
    count(*) as report_count,
    sum(case when is_delayed then 1 else 0 end) as delayed_count,
    count(distinct vehicle_id) as active_vehicles,
    count(distinct route_id) as active_routes,
    sum(case when is_delayed then 1 else 0 end) * 1.0 / nullif(count(*), 0) as delayed_rate
from positions
group by 1, 2
