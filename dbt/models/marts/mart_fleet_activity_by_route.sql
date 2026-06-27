-- Daily fleet activity: peak distinct vehicles and report volume per route.
with delays as (
    select * from {{ ref('int_delay_by_route_hour') }}
)

select
    agency,
    mode,
    route_id,
    route_name,
    report_date,
    max(active_vehicles) as peak_vehicles,
    sum(report_count) as reports
from delays
group by 1, 2, 3, 4, 5
