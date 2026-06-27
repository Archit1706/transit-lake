-- One row per route: dimensional attributes + headline OTP metrics.
with routes as (
    select * from {{ ref('dim_route') }}
),

otp as (
    select * from {{ ref('mart_otp_by_route') }}
)

select
    r.route_sk,
    r.agency,
    r.mode,
    r.route_id,
    r.route_name,
    r.route_type_name,
    coalesce(o.total_reports, 0) as total_reports,
    coalesce(o.delayed_reports, 0) as delayed_reports,
    o.peak_vehicles,
    o.on_time_rate
from routes r
left join otp o
    on r.mode = o.mode
   and r.route_id = o.route_id
