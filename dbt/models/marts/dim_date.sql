with spine as (
    select unnest(
        generate_series(DATE '2018-01-01', DATE '2026-12-31', INTERVAL '1 day')
    ) as ts
)

select
    cast(ts as DATE) as date_day,
    cast(strftime(ts, '%Y%m%d') as INTEGER) as date_key,
    extract(year from ts) as year,
    extract(quarter from ts) as quarter,
    extract(month from ts) as month,
    monthname(ts) as month_name,
    extract(day from ts) as day_of_month,
    extract(dow from ts) as day_of_week,
    dayname(ts) as day_name,
    extract(week from ts) as week_of_year,
    coalesce(extract(dow from ts) in (0, 6), false) as is_weekend
from spine
