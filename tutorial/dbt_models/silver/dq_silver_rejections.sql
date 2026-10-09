{{
  config(
    materialized='table',
    schema='GOVERNANCE',
    alias='SILVER_REJECTIONS',
    tags=['silver', 'dq']
  )
}}

WITH event_rejections AS (
    SELECT
        'clickstream' AS source_name,
        raw_payload:event_id::VARCHAR AS business_key,
        source_file,
        source_row_number,
        batch_id,
        raw_text,
        raw_payload,
        CASE
            WHEN NULLIF(TRIM(raw_payload:event_id::VARCHAR), '') IS NULL THEN 'MISSING_KEY'
            WHEN TRY_TO_TIMESTAMP_TZ(raw_payload:event_time::VARCHAR) IS NULL THEN 'INVALID_TIMESTAMP'
            WHEN COALESCE(LOWER(TRIM(raw_payload:event_type::VARCHAR)), '<NULL>') NOT IN ('view', 'add_to_cart', 'checkout', 'purchase') THEN 'INVALID_EVENT_TYPE'
            ELSE 'MISSING_ANALYTIC_USER'
        END AS error_code,
        'Clickstream record failed Silver validation' AS error_message,
        ingest_ts
    FROM {{ source('bronze', 'bronze_clickstream') }}
    WHERE is_parse_valid
      AND (
          NULLIF(TRIM(raw_payload:event_id::VARCHAR), '') IS NULL
          OR TRY_TO_TIMESTAMP_TZ(raw_payload:event_time::VARCHAR) IS NULL
          OR COALESCE(LOWER(TRIM(raw_payload:event_type::VARCHAR)), '<NULL>') NOT IN ('view', 'add_to_cart', 'checkout', 'purchase')
          OR COALESCE(
              NULLIF(TRIM(raw_payload:user_id::VARCHAR), ''),
              NULLIF(TRIM(raw_payload:session_id::VARCHAR), '')
          ) IS NULL
      )
),

order_rejections AS (
    SELECT
        'orders' AS source_name,
        raw_payload:order_id::VARCHAR AS business_key,
        source_file,
        source_row_number,
        batch_id,
        raw_text,
        raw_payload,
        CASE
            WHEN NULLIF(TRIM(raw_payload:order_id::VARCHAR), '') IS NULL THEN 'MISSING_KEY'
            WHEN NULLIF(TRIM(raw_payload:user_id::VARCHAR), '') IS NULL THEN 'MISSING_KEY'
            WHEN TRY_TO_TIMESTAMP_TZ(COALESCE(raw_payload:order_time::VARCHAR, raw_payload:created_at::VARCHAR)) IS NULL THEN 'INVALID_TIMESTAMP'
            WHEN TRY_TO_TIMESTAMP_TZ(COALESCE(raw_payload:updated_at::VARCHAR, raw_payload:order_time::VARCHAR)) IS NULL THEN 'INVALID_TIMESTAMP'
            WHEN NULLIF(TRIM(raw_payload:currency::VARCHAR), '') IS NULL THEN 'MISSING_CURRENCY'
            WHEN COALESCE(LOWER(TRIM(raw_payload:status::VARCHAR)), '<NULL>') NOT IN ('created', 'paid', 'shipped', 'cancelled', 'returned') THEN 'INVALID_STATUS'
            ELSE 'INVALID_ITEMS'
        END AS error_code,
        'Order record failed Silver validation' AS error_message,
        ingest_ts
    FROM {{ source('bronze', 'bronze_orders') }}
    WHERE is_parse_valid
      AND (
          NULLIF(TRIM(raw_payload:order_id::VARCHAR), '') IS NULL
          OR NULLIF(TRIM(raw_payload:user_id::VARCHAR), '') IS NULL
          OR TRY_TO_TIMESTAMP_TZ(COALESCE(raw_payload:order_time::VARCHAR, raw_payload:created_at::VARCHAR)) IS NULL
          OR TRY_TO_TIMESTAMP_TZ(COALESCE(raw_payload:updated_at::VARCHAR, raw_payload:order_time::VARCHAR)) IS NULL
          OR NULLIF(TRIM(raw_payload:currency::VARCHAR), '') IS NULL
          OR COALESCE(LOWER(TRIM(raw_payload:status::VARCHAR)), '<NULL>') NOT IN ('created', 'paid', 'shipped', 'cancelled', 'returned')
          OR COALESCE(TYPEOF(raw_payload:items), 'NULL') <> 'ARRAY'
      )
),

customer_rejections AS (
    SELECT
        'customers' AS source_name,
        raw_payload:user_id::VARCHAR AS business_key,
        source_file,
        source_row_number,
        batch_id,
        raw_text,
        raw_payload,
        CASE
            WHEN NULLIF(TRIM(raw_payload:user_id::VARCHAR), '') IS NULL THEN 'MISSING_KEY'
            WHEN TRY_TO_TIMESTAMP_TZ(raw_payload:updated_at::VARCHAR) IS NULL THEN 'INVALID_TIMESTAMP'
            WHEN NULLIF(TRIM(raw_payload:email::VARCHAR), '') IS NULL
             AND NULLIF(REGEXP_REPLACE(raw_payload:phone::VARCHAR, '[^0-9+]', ''), '') IS NULL THEN 'MISSING_CONTACT'
            ELSE 'INVALID_EMAIL'
        END AS error_code,
        'Customer record failed Silver validation' AS error_message,
        ingest_ts
    FROM {{ source('bronze', 'bronze_customers') }}
    WHERE is_parse_valid
      AND (
          NULLIF(TRIM(raw_payload:user_id::VARCHAR), '') IS NULL
          OR TRY_TO_TIMESTAMP_TZ(raw_payload:updated_at::VARCHAR) IS NULL
          OR (
              NULLIF(TRIM(raw_payload:email::VARCHAR), '') IS NULL
              AND NULLIF(REGEXP_REPLACE(raw_payload:phone::VARCHAR, '[^0-9+]', ''), '') IS NULL
          )
          OR (
              NULLIF(TRIM(raw_payload:email::VARCHAR), '') IS NOT NULL
              AND NOT REGEXP_LIKE(
                  LOWER(TRIM(raw_payload:email::VARCHAR)),
                  '^[A-Z0-9._%+-]+@[A-Z0-9.-]+[.][A-Z]{2,}$',
                  'i'
              )
          )
      )
),

product_rejections AS (
    SELECT
        'products' AS source_name,
        raw_payload:sku::VARCHAR AS business_key,
        source_file,
        source_row_number,
        batch_id,
        raw_text,
        raw_payload,
        CASE
            WHEN NULLIF(TRIM(raw_payload:sku::VARCHAR), '') IS NULL THEN 'MISSING_KEY'
            WHEN TRY_TO_TIMESTAMP_TZ(raw_payload:updated_at::VARCHAR) IS NULL THEN 'INVALID_TIMESTAMP'
            ELSE 'INVALID_PRODUCT'
        END AS error_code,
        'Product record failed Silver validation' AS error_message,
        ingest_ts
    FROM {{ source('bronze', 'bronze_products') }}
    WHERE is_parse_valid
      AND (
          NULLIF(TRIM(raw_payload:sku::VARCHAR), '') IS NULL
          OR NULLIF(TRIM(raw_payload:product_name::VARCHAR), '') IS NULL
          OR TRY_TO_DECIMAL(raw_payload:unit_cost::VARCHAR, 18, 4) < 0
          OR TRY_TO_DECIMAL(raw_payload:unit_cost::VARCHAR, 18, 4) IS NULL
          OR TRY_TO_TIMESTAMP_TZ(raw_payload:updated_at::VARCHAR) IS NULL
      )
),

fx_rejections AS (
    SELECT
        'fx_rates' AS source_name,
        CONCAT_WS('|', raw_payload:rate_date::VARCHAR, raw_payload:currency::VARCHAR) AS business_key,
        source_file,
        source_row_number,
        batch_id,
        raw_text,
        raw_payload,
        'INVALID_FX_RATE' AS error_code,
        'FX rate must have a date, currency and positive rate' AS error_message,
        ingest_ts
    FROM {{ source('bronze', 'bronze_fx_rates') }}
    WHERE is_parse_valid
      AND (
          TRY_TO_DATE(raw_payload:rate_date::VARCHAR) IS NULL
          OR NULLIF(TRIM(raw_payload:currency::VARCHAR), '') IS NULL
          OR TRY_TO_DOUBLE(raw_payload:rate_to_usd::VARCHAR) <= 0
          OR TRY_TO_DOUBLE(raw_payload:rate_to_usd::VARCHAR) IS NULL
      )
),

feedback_rejections AS (
    SELECT
        'customer_feedback' AS source_name,
        raw_payload:feedback_id::VARCHAR AS business_key,
        source_file,
        source_row_number,
        batch_id,
        raw_text,
        raw_payload,
        CASE
            WHEN NULLIF(TRIM(raw_payload:feedback_id::VARCHAR), '') IS NULL THEN 'MISSING_KEY'
            WHEN NULLIF(TRIM(raw_payload:user_id::VARCHAR), '') IS NULL THEN 'MISSING_KEY'
            WHEN NULLIF(TRIM(raw_payload:feedback_text::VARCHAR), '') IS NULL THEN 'EMPTY_TEXT'
            WHEN TRY_TO_TIMESTAMP_TZ(raw_payload:created_at::VARCHAR) IS NULL THEN 'INVALID_TIMESTAMP'
            WHEN TRY_TO_TIMESTAMP_TZ(COALESCE(raw_payload:updated_at::VARCHAR, raw_payload:created_at::VARCHAR)) IS NULL THEN 'INVALID_TIMESTAMP'
            ELSE 'PII_DETECTED'
        END AS error_code,
        'Feedback record requires validation or redaction' AS error_message,
        ingest_ts
    FROM {{ source('bronze', 'bronze_customer_feedback') }}
    WHERE is_parse_valid
      AND (
          NULLIF(TRIM(raw_payload:feedback_id::VARCHAR), '') IS NULL
          OR NULLIF(TRIM(raw_payload:user_id::VARCHAR), '') IS NULL
          OR NULLIF(TRIM(raw_payload:feedback_text::VARCHAR), '') IS NULL
          OR TRY_TO_TIMESTAMP_TZ(raw_payload:created_at::VARCHAR) IS NULL
          OR TRY_TO_TIMESTAMP_TZ(COALESCE(raw_payload:updated_at::VARCHAR, raw_payload:created_at::VARCHAR)) IS NULL
          OR REGEXP_LIKE(
              raw_payload:feedback_text::VARCHAR,
              '[A-Z0-9._%+-]+@[A-Z0-9.-]+[.][A-Z]{2,}',
              'i'
          )
      )
),

cdc_rejections AS (
    SELECT
        'cdc_deletes' AS source_name,
        raw_payload:business_key::VARCHAR AS business_key,
        source_file,
        source_row_number,
        batch_id,
        raw_text,
        raw_payload,
        'INVALID_CDC_DELETE' AS error_code,
        'CDC delete requires a supported entity, business key, D operation and changed_at' AS error_message,
        ingest_ts
    FROM {{ source('bronze', 'bronze_cdc_deletes') }}
    WHERE is_parse_valid
      AND (
          COALESCE(LOWER(raw_payload:entity_type::VARCHAR), '<NULL>') NOT IN ('order', 'orders', 'customer', 'customers')
          OR NULLIF(TRIM(raw_payload:business_key::VARCHAR), '') IS NULL
          OR COALESCE(UPPER(raw_payload:operation::VARCHAR), '<NULL>') <> 'D'
          OR TRY_TO_TIMESTAMP_TZ(raw_payload:changed_at::VARCHAR) IS NULL
      )
),

typed_order_items_for_dq AS (
    SELECT
        'order_items' AS source_name,
        CONCAT(orders.order_id, ':', item.index + 1) AS business_key,
        orders.source_file,
        item.index + 1 AS source_row_number,
        orders.batch_id,
        NULL::VARCHAR AS raw_text,
        item.value AS raw_payload,
        orders.ingest_ts,
        NULLIF(TRIM(item.value:sku::VARCHAR), '') AS sku,
        TRY_TO_NUMBER(item.value:qty::VARCHAR, 18, 3) AS qty,
        TRY_TO_DECIMAL(
            COALESCE(item.value:unit_price::VARCHAR, item.value:unit_price_original::VARCHAR),
            18,
            4
        ) AS unit_price_original
    FROM {{ ref('stg_orders_current') }} orders,
    LATERAL FLATTEN(INPUT => orders.items) item
    WHERE NOT orders.is_deleted
),

invalid_order_items AS (
    SELECT
        source_name,
        business_key,
        source_file,
        source_row_number,
        batch_id,
        raw_text,
        raw_payload,
        'INVALID_ORDER_ITEM' AS error_code,
        'Order item requires SKU, positive quantity and non-negative price' AS error_message,
        ingest_ts
    FROM typed_order_items_for_dq
    WHERE sku IS NULL
       OR qty <= 0
       OR qty IS NULL
       OR unit_price_original < 0
       OR unit_price_original IS NULL
),

unknown_skus AS (
    SELECT
        items.source_name,
        items.business_key,
        items.source_file,
        items.source_row_number,
        items.batch_id,
        items.raw_text,
        items.raw_payload,
        'UNKNOWN_SKU' AS error_code,
        'Order item SKU does not exist in the product master' AS error_message,
        items.ingest_ts
    FROM typed_order_items_for_dq items
    LEFT JOIN {{ ref('stg_products') }} products
      ON items.sku = products.sku
    WHERE items.sku IS NOT NULL
      AND items.qty > 0
      AND items.unit_price_original >= 0
      AND products.sku IS NULL
),

missing_fx AS (
    SELECT
        'order_items' AS source_name,
        CONCAT(order_id, ':', line_number) AS business_key,
        source_file,
        line_number AS source_row_number,
        batch_id,
        NULL::VARCHAR AS raw_text,
        TO_VARIANT(OBJECT_CONSTRUCT(
            'order_id', order_id,
            'line_number', line_number,
            'currency', currency,
            'order_time_utc', order_time_utc
        )) AS raw_payload,
        'FX_RATE_MISSING' AS error_code,
        'No FX rate found for order date and currency' AS error_message,
        ingest_ts
    FROM {{ ref('stg_order_items') }}
    WHERE is_fx_rate_missing
),

all_rejections AS (
    SELECT * FROM event_rejections
    UNION ALL SELECT * FROM order_rejections
    UNION ALL SELECT * FROM customer_rejections
    UNION ALL SELECT * FROM product_rejections
    UNION ALL SELECT * FROM fx_rejections
    UNION ALL SELECT * FROM feedback_rejections
    UNION ALL SELECT * FROM cdc_rejections
    UNION ALL SELECT * FROM invalid_order_items
    UNION ALL SELECT * FROM unknown_skus
    UNION ALL SELECT * FROM missing_fx
)

SELECT
    SHA2(
        CONCAT_WS(
            '|',
            source_name,
            COALESCE(source_file, '<DERIVED>'),
            COALESCE(source_row_number::VARCHAR, '<NULL>'),
            COALESCE(business_key, '<NULL>'),
            error_code
        ),
        256
    ) AS rejection_id,
    source_name,
    business_key,
    source_file,
    source_row_number,
    batch_id,
    raw_text,
    raw_payload,
    error_code,
    error_message,
    ingest_ts AS source_ingest_ts,
    CURRENT_TIMESTAMP() AS detected_at
FROM all_rejections
