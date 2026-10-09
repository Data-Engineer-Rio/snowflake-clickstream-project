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
    FROM {{ source('bronze', 'bronze_customers') }}
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
        NULLIF(TRIM(raw_payload:user_id::VARCHAR), '') AS user_id,
        NULLIF(LOWER(TRIM(raw_payload:email::VARCHAR)), '') AS email,
        NULLIF(REGEXP_REPLACE(raw_payload:phone::VARCHAR, '[^0-9+]', ''), '') AS phone,
        NULLIF(UPPER(TRIM(COALESCE(raw_payload:country_code::VARCHAR, raw_payload:country::VARCHAR))), '') AS country_code,
        TRY_TO_TIMESTAMP_TZ(raw_payload:created_at::VARCHAR) AS created_at,
        TRY_TO_TIMESTAMP_TZ(raw_payload:updated_at::VARCHAR) AS updated_at,
        source_file,
        source_row_number,
        batch_id,
        ingest_ts
    FROM source_rows
),

validated AS (
    SELECT
        *,
        SHA2(
            CONCAT_WS(
                '|',
                COALESCE(email, '<NULL>'),
                COALESCE(phone, '<NULL>'),
                COALESCE(country_code, '<NULL>')
            ),
            256
        ) AS record_hash
    FROM parsed
    WHERE user_id IS NOT NULL
      AND updated_at IS NOT NULL
      AND (email IS NOT NULL OR phone IS NOT NULL)
      AND (
          email IS NULL
          OR REGEXP_LIKE(email, '^[A-Z0-9._%+-]+@[A-Z0-9.-]+[.][A-Z]{2,}$', 'i')
      )
),

with_change_id AS (
    SELECT
        SHA2(CONCAT_WS('|', user_id, updated_at::VARCHAR, record_hash), 256) AS change_id,
        *
    FROM validated
)

SELECT *
FROM with_change_id
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY change_id
    ORDER BY ingest_ts DESC, source_file DESC, source_row_number DESC
) = 1
