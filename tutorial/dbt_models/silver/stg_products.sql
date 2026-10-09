{{
  config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='sku',
    on_schema_change='fail',
    tags=['silver', 'current']
  )
}}

WITH source_rows AS (
    SELECT *
    FROM {{ source('bronze', 'bronze_products') }}
    WHERE is_parse_valid
    {% if is_incremental() %}
      AND ingest_ts >= (
          SELECT DATEADD(
              DAY,
              -{{ var('silver_lookback_days', 7) }},
              COALESCE(MAX(ingest_ts), '1900-01-01'::TIMESTAMP_TZ)
          )
          FROM {{ this }}
      )
    {% endif %}
),

parsed AS (
    SELECT
        NULLIF(TRIM(raw_payload:sku::VARCHAR), '') AS sku,
        NULLIF(TRIM(raw_payload:product_name::VARCHAR), '') AS product_name,
        NULLIF(TRIM(raw_payload:brand::VARCHAR), '') AS brand,
        NULLIF(TRIM(raw_payload:subcategory_code::VARCHAR), '') AS subcategory_code,
        NULLIF(TRIM(raw_payload:subcategory_name::VARCHAR), '') AS subcategory_name,
        NULLIF(TRIM(raw_payload:category_code::VARCHAR), '') AS category_code,
        NULLIF(TRIM(raw_payload:category_name::VARCHAR), '') AS category_name,
        TRY_TO_DECIMAL(raw_payload:unit_cost::VARCHAR, 18, 4) AS unit_cost,
        TRY_TO_TIMESTAMP_TZ(raw_payload:updated_at::VARCHAR) AS updated_at,
        source_file,
        source_row_number,
        batch_id,
        ingest_ts,
        payload_hash AS record_hash
    FROM source_rows
),

source_winners AS (
    SELECT *
    FROM parsed
    WHERE sku IS NOT NULL
      AND product_name IS NOT NULL
      AND unit_cost >= 0
      AND updated_at IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY sku
        ORDER BY updated_at DESC, ingest_ts DESC, source_file DESC, source_row_number DESC
    ) = 1
)

{% if is_incremental() %}
SELECT candidate.*
FROM source_winners candidate
LEFT JOIN {{ this }} target
  ON candidate.sku = target.sku
WHERE target.sku IS NULL
   OR candidate.updated_at > target.updated_at
   OR (
       candidate.updated_at = target.updated_at
       AND candidate.ingest_ts > target.ingest_ts
   )
{% else %}
SELECT * FROM source_winners
{% endif %}
