with positions as (
    select * from {{ ref('stg_cta__vehicle_positions') }}
),

routes as (
    select * from {{ ref('stg_cta__routes') }}
)

select
    p.position_sk,
    p.agency,
    p.mode,
    p.vehicle_id,
    p.route_id,
    coalesce(r.route_long_name, p.route_id) as route_name,
    p.headsign,
    p.lat,
    p.lon,
    p.heading,
    p.is_delayed,
    p.report_ts,
    p.report_date,
    p.report_hour
from positions p
left join routes r
    on p.route_id = r.route_id
    and p.mode = 'bus'
