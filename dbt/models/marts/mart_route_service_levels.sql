-- Scheduled service levels per route: trip and stop-event counts from the timetable.
with trips as (
    select * from {{ ref('stg_cta__trips') }}
),

stop_times as (
    select trip_id, count(*) as stop_events
    from {{ ref('stg_cta__stop_times') }}
    group by trip_id
),

routes as (
    select * from {{ ref('stg_cta__routes') }}
)

select
    t.route_id,
    r.route_long_name as route_name,
    count(distinct t.trip_id) as scheduled_trips,
    count(distinct t.service_id) as service_patterns,
    count(distinct t.shape_id) as shapes,
    sum(coalesce(st.stop_events, 0)) as total_stop_events
from trips t
left join stop_times st using (trip_id)
left join routes r on t.route_id = r.route_id
group by 1, 2
order by scheduled_trips desc
