{{
  config(
    materialized='table',
    schema='GOLD',
    tags=['gold', 'dimension']
  )
}}

WITH source_dates AS (
    SELECT TO_DATE(event_time_utc) AS calendar_date
    FROM {{ ref('stg_events') }}

    UNION ALL

    SELECT TO_DATE(order_time_utc) AS calendar_date
    FROM {{ ref('stg_order_changes') }}

    UNION ALL

    SELECT TO_DATE(created_at) AS calendar_date
    FROM {{ ref('stg_customer_feedback') }}

    UNION ALL

    SELECT TO_DATE(status_at) AS calendar_date
    FROM {{ ref('stg_order_status_history') }}

    UNION ALL

    SELECT TO_DATE(quarantined_at) AS calendar_date
    FROM {{ source('governance', 'quarantine_records') }}
),

boundaries AS (
    SELECT
        COALESCE(MIN(calendar_date), CURRENT_DATE()) AS min_date,
        COALESCE(MAX(calendar_date), CURRENT_DATE()) AS max_date
    FROM source_dates
),

day_numbers AS (
    SELECT ROW_NUMBER() OVER (ORDER BY SEQ4()) - 1 AS day_offset
    FROM TABLE(GENERATOR(ROWCOUNT => 36600))
),

calendar AS (
    SELECT DATEADD(DAY, numbers.day_offset, boundaries.min_date) AS calendar_date
    FROM day_numbers numbers
    CROSS JOIN boundaries
    WHERE numbers.day_offset <= DATEDIFF(DAY, boundaries.min_date, boundaries.max_date)
)

SELECT
    TO_NUMBER(TO_CHAR(calendar_date, 'YYYYMMDD')) AS date_key,
    calendar_date,
    YEAR(calendar_date) AS calendar_year,
    QUARTER(calendar_date) AS calendar_quarter,
    MONTH(calendar_date) AS calendar_month,
    MONTHNAME(calendar_date) AS month_name,
    WEEKISO(calendar_date) AS iso_week_of_year,
    DAY(calendar_date) AS day_of_month,
    DAYOFWEEKISO(calendar_date) AS iso_day_of_week,
    DAYNAME(calendar_date) AS day_name,
    DATE_TRUNC('WEEK', calendar_date) AS week_start_date,
    DATE_TRUNC('MONTH', calendar_date) AS month_start_date,
    DAYOFWEEKISO(calendar_date) IN (6, 7) AS is_weekend
FROM calendar
