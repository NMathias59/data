{{ config(
    materialized='table'
) }}

WITH date_range AS (
    -- Générer une plage de dates couvrant toutes les données
    SELECT
        arrayJoin(arrayMap(x -> toDate('2016-01-01') + INTERVAL x DAY, range(0, datediff('day', toDate('2016-01-01'), today() + INTERVAL 365 DAY)))) as date
),

date_attributes AS (
    SELECT
        date,
        toYear(date) as year,
        toMonth(date) as month,
        toDayOfMonth(date) as day,
        toDayOfWeek(date) as day_of_week,
        toDayOfYear(date) as day_of_year,
        toQuarter(date) as quarter,
        formatDateTime(date, '%Y-%m') as year_month,
        formatDateTime(date, '%Y-Q%Q') as year_quarter,
        CASE
            WHEN toDayOfWeek(date) IN (6, 7) THEN 'Weekend'
            ELSE 'Weekday'
        END as day_type,
        CASE toMonth(date)
            WHEN 1 THEN 'January'
            WHEN 2 THEN 'February'
            WHEN 3 THEN 'March'
            WHEN 4 THEN 'April'
            WHEN 5 THEN 'May'
            WHEN 6 THEN 'June'
            WHEN 7 THEN 'July'
            WHEN 8 THEN 'August'
            WHEN 9 THEN 'September'
            WHEN 10 THEN 'October'
            WHEN 11 THEN 'November'
            WHEN 12 THEN 'December'
        END as month_name,
        CASE toDayOfWeek(date)
            WHEN 1 THEN 'Monday'
            WHEN 2 THEN 'Tuesday'
            WHEN 3 THEN 'Wednesday'
            WHEN 4 THEN 'Thursday'
            WHEN 5 THEN 'Friday'
            WHEN 6 THEN 'Saturday'
            WHEN 7 THEN 'Sunday'
        END as day_name
    FROM date_range
)

SELECT
    date as date_key,
    year,
    month,
    day,
    day_of_week,
    day_of_year,
    quarter,
    year_month,
    year_quarter,
    day_type,
    month_name,
    day_name,
    -- Indicateurs business
    CASE WHEN day_of_week IN (6, 7) THEN 1 ELSE 0 END as is_weekend,
    CASE WHEN month IN (12, 1, 2) THEN 'Winter'
         WHEN month IN (3, 4, 5) THEN 'Spring'
         WHEN month IN (6, 7, 8) THEN 'Summer'
         ELSE 'Fall' END as season,
    CASE WHEN month IN (11, 12, 1, 2) THEN 'Q4'
         WHEN month IN (2, 3, 4) THEN 'Q1'
         WHEN month IN (5, 6, 7) THEN 'Q2'
         ELSE 'Q3' END as fiscal_quarter
FROM date_attributes
ORDER BY date
