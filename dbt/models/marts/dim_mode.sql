-- Transit mode dimension.
select 'bus' as mode, 'Bus' as mode_name, 'CTA bus fleet (BusTime)' as description
union all
select 'train' as mode, 'Rail' as mode_name, 'CTA "L" rail lines (Train Tracker)' as description
