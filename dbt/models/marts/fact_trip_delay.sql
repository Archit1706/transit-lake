-- Fact: position reports flagged delayed by the agency feed (BusTime dly / Train
-- Tracker isDly). Grain = one delayed report; the basis for on-time-performance.
with positions as (
    select * from {{ ref('int_positions_enriched') }}
    where is_delayed
)

select
    position_sk,
    {{ surrogate_key(['mode', 'route_id']) }} as route_sk,
    cast(strftime(report_ts, '%Y%m%d') as integer) as date_key,
    agency,
    mode,
    route_id,
    route_name,
    vehicle_id,
    lat,
    lon,
    report_ts,
    report_date,
    report_hour
from positions
