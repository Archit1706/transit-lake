-- Fact: one row per road-segment congestion observation.
with congestion as (
    select * from {{ ref('stg_chicago__congestion') }}
)

select
    {{ surrogate_key(['segment_id', 'report_ts']) }} as congestion_sk,
    {{ surrogate_key(['segment_id']) }} as segment_sk,
    cast(strftime(report_ts, '%Y%m%d') as integer) as date_key,
    segment_id,
    report_ts,
    report_date,
    report_hour,
    day_of_week,
    speed_mph,
    bus_count,
    message_count,
    length_mi
from congestion
