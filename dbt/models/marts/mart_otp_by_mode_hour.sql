-- On-time performance by mode and hour of day (rush-hour signal).
with delays as (
    select * from {{ ref('int_delay_by_route_hour') }}
)

select
    mode,
    report_hour,
    sum(report_count) as total_reports,
    sum(delayed_count) as delayed_reports,
    1 - sum(delayed_count) * 1.0 / nullif(sum(report_count), 0) as on_time_rate
from delays
group by 1, 2
order by mode, report_hour
