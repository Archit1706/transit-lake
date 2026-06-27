-- Daily delay-rate trend per mode, with weekday context from dim_date.
with delay as (
    select * from {{ ref('int_daily_transit_delay') }}
),

dates as (
    select * from {{ ref('dim_date') }}
)

select
    d.report_date,
    dd.day_name,
    dd.is_weekend,
    d.mode,
    d.report_count,
    d.delayed_count,
    d.delayed_rate,
    d.active_vehicles,
    d.active_routes
from delay d
left join dates dd
    on d.report_date = dd.date_day
order by d.report_date, d.mode
