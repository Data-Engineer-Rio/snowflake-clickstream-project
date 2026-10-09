{{
  config(
    materialized='table',
    schema='GOLD',
    tags=['gold', 'mart', 'customer_360']
  )
}}

WITH order_metrics AS (
    SELECT
        customers.user_id,
        COUNT(DISTINCT facts.order_id) AS total_orders,
        COUNT(DISTINCT IFF(
            facts.order_status NOT IN ('cancelled', 'returned'),
            facts.order_id,
            NULL
        )) AS completed_orders,
        SUM(COALESCE(facts.net_revenue_usd, 0))::NUMBER(38, 4) AS lifetime_revenue_usd,
        MIN(IFF(
            facts.order_status NOT IN ('cancelled', 'returned'),
            facts.order_time_utc,
            NULL
        )) AS first_purchase_at,
        MAX(IFF(
            facts.order_status NOT IN ('cancelled', 'returned'),
            facts.order_time_utc,
            NULL
        )) AS last_purchase_at
    FROM {{ ref('fact_order_line') }} facts
    INNER JOIN {{ ref('dim_customer') }} customers
      ON customers.customer_key = facts.customer_key
    WHERE customers.user_id <> '<UNKNOWN>'
    GROUP BY customers.user_id
),

device_counts AS (
    SELECT
        user_id,
        device_os,
        COUNT(*) AS event_count,
        MAX(event_time_utc) AS last_event_at
    FROM {{ ref('fact_customer_event') }}
    WHERE user_id IS NOT NULL
    GROUP BY user_id, device_os
),

favorite_device AS (
    SELECT
        user_id,
        device_os AS favorite_device_os,
        last_event_at
    FROM device_counts
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY user_id
        ORDER BY event_count DESC, last_event_at DESC, device_os
    ) = 1
),

feedback_metrics AS (
    SELECT
        user_id,
        COUNT(*) AS feedback_count,
        MAX(created_at) AS last_feedback_at,
        COUNT_IF(contains_pii) AS pii_feedback_count
    FROM {{ ref('stg_customer_feedback') }}
    GROUP BY user_id
)

SELECT
    customers.customer_key,
    customers.user_id,
    customers.email,
    customers.phone,
    customers.country_code,
    customers.created_at,
    customers.is_deleted,
    customers.deleted_at,
    COALESCE(orders.total_orders, 0) AS total_orders,
    COALESCE(orders.completed_orders, 0) AS completed_orders,
    COALESCE(orders.lifetime_revenue_usd, 0)::NUMBER(38, 4) AS lifetime_revenue_usd,
    orders.first_purchase_at,
    orders.last_purchase_at,
    devices.favorite_device_os,
    devices.last_event_at,
    COALESCE(feedback.feedback_count, 0) AS feedback_count,
    feedback.last_feedback_at,
    COALESCE(feedback.pii_feedback_count, 0) AS pii_feedback_count,
    CURRENT_TIMESTAMP() AS dbt_loaded_at
FROM {{ ref('dim_customer') }} customers
LEFT JOIN order_metrics orders
  ON orders.user_id = customers.user_id
LEFT JOIN favorite_device devices
  ON devices.user_id = customers.user_id
LEFT JOIN feedback_metrics feedback
  ON feedback.user_id = customers.user_id
WHERE customers.is_current
  AND customers.user_id <> '<UNKNOWN>'
