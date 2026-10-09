{{
  config(
    materialized='table',
    schema='GOLD',
    tags=['gold', 'fact']
  )
}}

SELECT
    history.status_event_id AS order_status_key,
    history.status_event_id,
    history.order_id,
    TO_NUMBER(TO_CHAR(TO_DATE(history.status_at), 'YYYYMMDD')) AS status_date_key,
    COALESCE(customers.customer_key, SHA2('CUSTOMER|<UNKNOWN>', 256)) AS customer_key,
    COALESCE(geographies.geography_key, SHA2('GEOGRAPHY|<UNKNOWN>', 256)) AS geography_key,
    orders.user_id,
    history.status,
    history.status_at,
    history.is_delete_event,
    history.source_file,
    history.source_row_number,
    history.batch_id,
    history.ingest_ts,
    history.record_hash
FROM {{ ref('stg_order_status_history') }} history
LEFT JOIN {{ ref('stg_orders_current') }} orders
  ON orders.order_id = history.order_id
LEFT JOIN {{ ref('dim_customer') }} customers
  ON customers.user_id = orders.user_id
 AND history.status_at >= customers.effective_from
 AND history.status_at < COALESCE(customers.effective_to, '9999-12-31'::TIMESTAMP_TZ)
 AND customers.user_id <> '<UNKNOWN>'
LEFT JOIN {{ ref('dim_geography') }} geographies
  ON geographies.country_code = orders.country_code
 AND NOT geographies.is_unknown
