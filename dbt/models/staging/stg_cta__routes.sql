with source as (
    select * from {{ source('silver', 'silver_routes') }}
)

select
    agency,
    route_id,
    route_short_name,
    route_long_name,
    route_type,
    case route_type
        when 0 then 'tram'
        when 1 then 'subway'
        when 2 then 'rail'
        when 3 then 'bus'
        else 'other'
    end as route_type_name,
    route_color,
    route_text_color
from source
