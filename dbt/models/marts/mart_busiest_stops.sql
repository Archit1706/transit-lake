-- Busiest stops by scheduled departures (from the static timetable).
with stop_times as (
    select * from {{ ref('stg_cta__stop_times') }}
),

stops as (
    select * from {{ ref('dim_stop') }}
)

select
    s.stop_id,
    s.stop_name,
    s.stop_lat,
    s.stop_lon,
    count(*) as scheduled_departures,
    count(distinct st.trip_id) as distinct_trips
from stop_times st
inner join stops s using (stop_id)
group by 1, 2, 3, 4
order by scheduled_departures desc
