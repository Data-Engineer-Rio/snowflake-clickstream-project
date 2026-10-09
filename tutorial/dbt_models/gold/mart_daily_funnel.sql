{{
  config(
    materialized='table',
    schema='GOLD',
    tags=['gold', 'mart', 'funnel']
  )
}}

WITH daily_steps AS (
    SELECT
        TO_DATE(event_time_utc) AS report_date,
        device_os,
        COUNT(DISTINCT IFF(event_type = 'view', analytic_user_id, NULL)) AS view_users,
        COUNT(DISTINCT IFF(event_type = 'add_to_cart', analytic_user_id, NULL)) AS add_to_cart_users,
        COUNT(DISTINCT IFF(event_type = 'checkout', analytic_user_id, NULL)) AS checkout_users,
        COUNT(DISTINCT IFF(event_type = 'purchase', analytic_user_id, NULL)) AS purchase_users
    FROM {{ ref('fact_customer_event') }}
    GROUP BY TO_DATE(event_time_utc), device_os
)

SELECT
    TO_NUMBER(TO_CHAR(report_date, 'YYYYMMDD')) AS date_key,
    report_date,
    device_os,
    view_users,
    add_to_cart_users,
    checkout_users,
    purchase_users,
    DIV0(add_to_cart_users, view_users) AS view_to_cart_rate,
    DIV0(checkout_users, add_to_cart_users) AS cart_to_checkout_rate,
    DIV0(purchase_users, checkout_users) AS checkout_to_purchase_rate,
    DIV0(purchase_users, view_users) AS view_to_purchase_rate
FROM daily_steps
