-- On-time performance by route x hour — drives the per-route heatmap.
with delays as (
    select * from {{ ref('int_delay_by_route_hour') }}
)

select
    agency,
    mode,
    route_id,
    route_name,
    report_hour,
    sum(report_count) as total_reports,
    sum(delayed_count) as delayed_reports,
    1 - sum(delayed_count) * 1.0 / nullif(sum(report_count), 0) as on_time_rate
from delays
group by 1, 2, 3, 4, 5
