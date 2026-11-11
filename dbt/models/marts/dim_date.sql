-- Calendar dimension covering every order date (plus a day of headroom so a
-- run that crosses midnight never produces an orphan date key).
with bounds as (
    select min(cast(ordered_at as date)) as first_day,
           greatest(max(cast(ordered_at as date)), current_date) + interval '1' day as last_day
    from {{ ref('stg_orders') }}
),
days as (
    select d as date_day
    from bounds
    cross join unnest(sequence(first_day, last_day, interval '1' day)) as t(d)
)
select
    cast(date_format(date_day, '%Y%m%d') as integer) as date_key,
    date_day,
    year(date_day) as year,
    quarter(date_day) as quarter,
    month(date_day) as month,
    date_format(date_day, '%M') as month_name,
    day_of_week(date_day) as iso_day_of_week,
    day_of_week(date_day) in (6, 7) as is_weekend
from days
