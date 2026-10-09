{{
  config(
    materialized='table',
    schema='GOLD',
    tags=['gold', 'fact']
  )
}}

SELECT
    SHA2(CONCAT_WS('|', 'ORDER_LINE', items.order_id, items.line_number::VARCHAR), 256) AS order_line_key,
    items.order_id,
    items.line_number,
    TO_NUMBER(TO_CHAR(TO_DATE(orders.order_time_utc), 'YYYYMMDD')) AS order_date_key,
    COALESCE(customers.customer_key, SHA2('CUSTOMER|<UNKNOWN>', 256)) AS customer_key,
    products.product_key,
    COALESCE(geographies.geography_key, SHA2('GEOGRAPHY|<UNKNOWN>', 256)) AS geography_key,
    orders.user_id,
    items.sku,
    orders.status AS order_status,
    items.qty,
    items.currency,
    items.unit_price_original,
    items.fx_rate_to_usd,
    items.line_amount_original,
    items.line_amount_usd AS gross_revenue_usd,
    IFF(
        orders.status IN ('cancelled', 'returned'),
        0::NUMBER(18, 4),
        items.line_amount_usd
    ) AS net_revenue_usd,
    items.is_fx_rate_missing,
    orders.order_time_utc,
    orders.updated_at AS order_updated_at,
    items.source_file,
    items.batch_id,
    items.ingest_ts,
    CURRENT_TIMESTAMP() AS dbt_loaded_at
FROM {{ ref('stg_order_items') }} items
INNER JOIN {{ ref('stg_orders_current') }} orders
  ON orders.order_id = items.order_id
 AND NOT orders.is_deleted
INNER JOIN {{ ref('dim_product') }} products
  ON products.sku = items.sku
LEFT JOIN {{ ref('dim_customer') }} customers
  ON customers.user_id = orders.user_id
 AND orders.order_time_utc >= customers.effective_from
 AND orders.order_time_utc < COALESCE(customers.effective_to, '9999-12-31'::TIMESTAMP_TZ)
 AND customers.user_id <> '<UNKNOWN>'
LEFT JOIN {{ ref('dim_geography') }} geographies
  ON geographies.country_code = orders.country_code
 AND NOT geographies.is_unknown
