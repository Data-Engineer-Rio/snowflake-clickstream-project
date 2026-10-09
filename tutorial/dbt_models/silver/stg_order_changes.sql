{{
  config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='change_id',
    on_schema_change='fail',
    tags=['silver', 'history']
  )
}}

WITH source_rows AS (
    SELECT *
    FROM {{ source('bronze', 'bronze_orders') }}
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
        NULLIF(TRIM(raw_payload:order_id::VARCHAR), '') AS order_id,
        TRY_TO_TIMESTAMP_TZ(COALESCE(raw_payload:order_time::VARCHAR, raw_payload:created_at::VARCHAR)) AS order_time_utc,
        NULLIF(TRIM(raw_payload:user_id::VARCHAR), '') AS user_id,
        UPPER(TRIM(raw_payload:currency::VARCHAR)) AS currency,
        LOWER(TRIM(raw_payload:status::VARCHAR)) AS status,
        UPPER(TRIM(COALESCE(raw_payload:country_code::VARCHAR, raw_payload:country::VARCHAR))) AS country_code,
        TRY_TO_TIMESTAMP_TZ(COALESCE(raw_payload:updated_at::VARCHAR, raw_payload:order_time::VARCHAR)) AS updated_at,
        raw_payload:items AS items,
        source_file,
        source_row_number,
        batch_id,
        ingest_ts,
        payload_hash AS record_hash
    FROM source_rows
),

valid AS (
    SELECT
        SHA2(
            CONCAT_WS(
                '|',
                order_id,
                updated_at::VARCHAR,
                COALESCE(record_hash, '<NULL>')
            ),
            256
        ) AS change_id,
        *
    FROM parsed
    WHERE order_id IS NOT NULL
      AND order_time_utc IS NOT NULL
      AND user_id IS NOT NULL
      AND currency IS NOT NULL
      AND updated_at IS NOT NULL
      AND status IN ('created', 'paid', 'shipped', 'cancelled', 'returned')
      AND TYPEOF(items) = 'ARRAY'
)

SELECT *
FROM valid
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY change_id
    ORDER BY ingest_ts DESC, source_file DESC, source_row_number DESC
) = 1
