-- Multi-modal edge: transit delay vs road congestion by hour-of-day profile.
-- Joined on hour (not date) because the historical congestion slice and the live
-- RT feed don't share a calendar window — the time-of-day pattern is the signal.
with transit as (
    select
        report_hour,
        sum(report_count) as transit_reports,
        sum(delayed_count) as delayed_reports,
        sum(delayed_count) * 1.0 / nullif(sum(report_count), 0) as delayed_rate
    from {{ ref('int_delay_by_route_hour') }}
    group by report_hour
),

road as (
    select
        report_hour,
        avg(avg_speed_mph) as road_avg_speed_mph,
        count(*) as segment_hours
    from {{ ref('int_congestion_segment_hour') }}
    group by report_hour
)

select
    t.report_hour,
    t.transit_reports,
    t.delayed_rate,
    r.road_avg_speed_mph,
    r.segment_hours
from transit t
left join road r using (report_hour)
order by t.report_hour
