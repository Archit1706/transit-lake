-- Route dimension covering every GTFS bus route plus any route id seen in the
-- real-time feeds (all rail lines, plus any bus route not in the static snapshot).
with routes as (
    select * from {{ ref('stg_cta__routes') }}
),

seen as (
    select distinct mode, route_id from {{ ref('stg_cta__vehicle_positions') }}
),

unioned as (
    select
        'bus' as mode,
        route_id,
        route_long_name as route_name,
        route_short_name,
        route_type_name,
        route_color
    from routes

    union

    select
        s.mode,
        s.route_id,
        s.route_id as route_name,
        null as route_short_name,
        case when s.mode = 'train' then 'subway' else 'bus' end as route_type_name,
        null as route_color
    from seen s
    left join routes r
        on s.route_id = r.route_id
       and s.mode = 'bus'
    where r.route_id is null
)

select
    {{ surrogate_key(['mode', 'route_id']) }} as route_sk,
    'CTA' as agency,
    mode,
    route_id,
    route_name,
    route_short_name,
    route_type_name,
    route_color
from unioned
