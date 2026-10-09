{{
  config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='feedback_id',
    on_schema_change='fail',
    tags=['silver', 'current']
  )
}}

WITH source_rows AS (
    SELECT *
    FROM {{ source('bronze', 'bronze_customer_feedback') }}
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
        NULLIF(TRIM(raw_payload:feedback_id::VARCHAR), '') AS feedback_id,
        NULLIF(TRIM(raw_payload:user_id::VARCHAR), '') AS user_id,
        NULLIF(TRIM(raw_payload:order_id::VARCHAR), '') AS order_id,
        raw_payload:feedback_text::VARCHAR AS feedback_text,
        TRY_TO_TIMESTAMP_TZ(raw_payload:created_at::VARCHAR) AS created_at,
        TRY_TO_TIMESTAMP_TZ(COALESCE(raw_payload:updated_at::VARCHAR, raw_payload:created_at::VARCHAR)) AS updated_at,
        LOWER(TRIM(raw_payload:language::VARCHAR)) AS language,
        REGEXP_LIKE(
            raw_payload:feedback_text::VARCHAR,
            '[A-Z0-9._%+-]+@[A-Z0-9.-]+[.][A-Z]{2,}',
            'i'
        ) AS contains_pii,
        REGEXP_REPLACE(
            raw_payload:feedback_text::VARCHAR,
            '[A-Z0-9._%+-]+@[A-Z0-9.-]+[.][A-Z]{2,}',
            '[REDACTED_EMAIL]',
            1,
            0,
            'i'
        ) AS feedback_text_redacted,
        source_file,
        source_row_number,
        batch_id,
        ingest_ts
    FROM source_rows
),

source_winners AS (
    SELECT
        feedback_id,
        user_id,
        order_id,
        feedback_text,
        feedback_text_redacted,
        contains_pii,
        created_at,
        updated_at,
        language,
        SHA2(feedback_text, 256) AS feedback_hash,
        source_file,
        source_row_number,
        batch_id,
        ingest_ts
    FROM parsed
    WHERE feedback_id IS NOT NULL
      AND user_id IS NOT NULL
      AND NULLIF(TRIM(feedback_text), '') IS NOT NULL
      AND created_at IS NOT NULL
      AND updated_at IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY feedback_id
        ORDER BY updated_at DESC, ingest_ts DESC, source_file DESC, source_row_number DESC
    ) = 1
)

{% if is_incremental() %}
SELECT candidate.*
FROM source_winners candidate
LEFT JOIN {{ this }} target
  ON candidate.feedback_id = target.feedback_id
WHERE target.feedback_id IS NULL
   OR candidate.updated_at > target.updated_at
   OR (
       candidate.updated_at = target.updated_at
       AND candidate.ingest_ts > target.ingest_ts
   )
{% else %}
SELECT * FROM source_winners
{% endif %}
