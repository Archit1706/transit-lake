-- Daily transit delay rate joined to that day's weather (delay ↔ weather).
with delay as (
    select * from {{ ref('int_daily_transit_delay') }}
),

weather as (
    select * from {{ ref('fact_weather_daily') }}
)

select
    d.report_date,
    d.mode,
    d.report_count,
    d.delayed_count,
    d.delayed_rate,
    d.active_vehicles,
    w.temp_avg_c,
    w.precip_mm,
    w.snow_cm,
    w.wind_kmh,
    w.is_wet_day,
    w.is_snow_day
from delay d
left join weather w
    on d.report_date = w.weather_date
order by d.report_date, d.mode
