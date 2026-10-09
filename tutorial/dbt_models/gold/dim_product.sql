{{
  config(
    materialized='table',
    schema='GOLD',
    tags=['gold', 'dimension', 'scd1']
  )
}}

SELECT
    SHA2(CONCAT('PRODUCT|', sku), 256) AS product_key,
    sku,
    product_name,
    COALESCE(NULLIF(TRIM(brand), ''), '<UNKNOWN>') AS brand,
    COALESCE(NULLIF(TRIM(subcategory_code), ''), '<UNKNOWN>') AS subcategory_code,
    COALESCE(NULLIF(TRIM(subcategory_name), ''), '<UNKNOWN>') AS subcategory_name,
    COALESCE(NULLIF(TRIM(category_code), ''), '<UNKNOWN>') AS category_code,
    COALESCE(NULLIF(TRIM(category_name), ''), '<UNKNOWN>') AS category_name,
    unit_cost AS unit_cost_usd,
    updated_at AS source_updated_at,
    TRUE AS is_current,
    record_hash,
    CURRENT_TIMESTAMP() AS dbt_loaded_at
FROM {{ ref('stg_products') }}
