-- Dimension: a generated calendar, ONE row per day (grain = a date).
--
-- Generated, NOT derived from events. dbt_utils.date_spine emits a CONTIGUOUS
-- range of dates, so a day with zero events still gets a row. Building this from
-- SELECT DISTINCT event_date would leave gaps that silently break date-range
-- joins/rollups in the marts. dim_date is a CONFORMED dimension -- shared by every
-- fact/mart with one consistent meaning -- so it must be complete, not event-driven.
--
-- date_key (YYYYMMDD int) is a SMART key: the deliberate exception to the
-- "surrogate keys should be meaningless" rule. A readable, sortable integer date
-- key is the long-standing Kimball convention (human-readable fact rows + cheap
-- range filters), so this dim does NOT use generate_surrogate_key.
--
-- Range = all of 2024 (the backfill window). end_date is EXCLUSIVE in date_spine,
-- so 2025-01-01 yields through 2024-12-31 (366 rows -- 2024 is a leap year).

with spine as (

    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2024-01-01' as date)",
        end_date="cast('2025-01-01' as date)"
    ) }}

),

calendar as (

    select cast(date_day as date) as full_date
    from spine

)

select
    cast(format_date('%Y%m%d', full_date) as int64) as date_key,
    full_date,
    extract(year from full_date) as year,
    extract(quarter from full_date) as quarter,
    extract(month from full_date) as month,
    extract(day from full_date) as day,
    -- BigQuery DAYOFWEEK: 1 = Sunday ... 7 = Saturday
    extract(dayofweek from full_date) as day_of_week,
    format_date('%A', full_date) as day_name,
    extract(dayofweek from full_date) in (1, 7) as is_weekend
from calendar
