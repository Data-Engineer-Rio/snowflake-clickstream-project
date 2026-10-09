{{
  config(
    materialized='table',
    tags=['silver']
  )
}}

WITH flattened AS (
    SELECT
        orders.order_id,
        item.index + 1 AS line_number,
        NULLIF(TRIM(item.value:sku::VARCHAR), '') AS sku,
        TRY_TO_NUMBER(item.value:qty::VARCHAR, 18, 3) AS qty,
        TRY_TO_DECIMAL(
            COALESCE(item.value:unit_price::VARCHAR, item.value:unit_price_original::VARCHAR),
            18,
            4
        ) AS unit_price_original,
        orders.currency,
        orders.order_time_utc,
        orders.updated_at,
        orders.source_file,
        orders.batch_id,
        orders.ingest_ts
    FROM {{ ref('stg_orders_current') }} orders,
    LATERAL FLATTEN(INPUT => orders.items) item
    WHERE NOT orders.is_deleted
),

valid_items AS (
    SELECT *
    FROM flattened
    WHERE sku IS NOT NULL
      AND qty > 0
      AND unit_price_original >= 0
),

enriched AS (
    SELECT
        items.*,
        rates.rate AS fx_rate_to_usd
    FROM valid_items items
    INNER JOIN {{ ref('stg_products') }} products
      ON items.sku = products.sku
    LEFT JOIN {{ ref('stg_fx_rates') }} rates
      ON rates.rate_date = TO_DATE(items.order_time_utc)
     AND rates.source_currency = items.currency
     AND rates.target_currency = 'USD'
)

SELECT
    order_id,
    line_number,
    sku,
    qty,
    unit_price_original,
    currency,
    fx_rate_to_usd,
    CAST(qty * unit_price_original AS NUMBER(18, 4)) AS line_amount_original,
    CAST(qty * unit_price_original * fx_rate_to_usd AS NUMBER(18, 4)) AS line_amount_usd,
    fx_rate_to_usd IS NULL AS is_fx_rate_missing,
    order_time_utc,
    updated_at,
    source_file,
    batch_id,
    ingest_ts
FROM enriched
