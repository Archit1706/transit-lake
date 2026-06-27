-- Fact: one row per day of Chicago weather, keyed to dim_date.
with weather as (
    select * from {{ ref('stg_open_meteo__weather') }}
)

select
    cast(strftime(weather_date, '%Y%m%d') as integer) as date_key,
    weather_date,
    temp_max_c,
    temp_min_c,
    temp_avg_c,
    precip_mm,
    rain_mm,
    snow_cm,
    wind_kmh,
    gust_kmh,
    is_wet_day,
    is_snow_day
from weather
