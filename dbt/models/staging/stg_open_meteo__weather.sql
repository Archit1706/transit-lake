with source as (
    select * from {{ source('silver', 'silver_weather') }}
)

select
    weather_date,
    temp_max_c,
    temp_min_c,
    (temp_max_c + temp_min_c) / 2.0 as temp_avg_c,
    precip_mm,
    rain_mm,
    snow_cm,
    wind_kmh,
    gust_kmh,
    precip_mm > 0 as is_wet_day,
    snow_cm > 0 as is_snow_day
from source
