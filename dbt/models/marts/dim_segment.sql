-- Road-segment dimension from the congestion feed (one row per segment).
with congestion as (
    select * from {{ ref('stg_chicago__congestion') }}
)

select
    segment_id,
    {{ surrogate_key(['segment_id']) }} as segment_sk,
    any_value(street) as street,
    any_value(direction) as direction,
    any_value(from_street) as from_street,
    any_value(to_street) as to_street,
    avg(length_mi) as length_mi,
    avg(start_lat) as start_lat,
    avg(start_lon) as start_lon,
    avg(end_lat) as end_lat,
    avg(end_lon) as end_lon
from congestion
group by segment_id
