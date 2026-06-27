-- Observability mart: latest data timestamp and row count per fact source.
select 'vehicle_positions' as source, max(report_ts) as latest_ts, count(*) as row_count
from {{ ref('fact_vehicle_position') }}
union all
select 'trip_delay', max(report_ts), count(*)
from {{ ref('fact_trip_delay') }}
union all
select 'congestion', max(report_ts), count(*)
from {{ ref('fact_congestion') }}
union all
select 'weather', max(cast(weather_date as timestamp)), count(*)
from {{ ref('fact_weather_daily') }}
