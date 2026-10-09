{{
  config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='delete_event_id',
    on_schema_change='fail',
    tags=['silver', 'cdc']
  )
}}

WITH source_rows AS (
    SELECT *
    FROM {{ source('bronze', 'bronze_cdc_deletes') }}
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
        CASE
            WHEN LOWER(raw_payload:entity_type::VARCHAR) IN ('order', 'orders') THEN 'order'
            WHEN LOWER(raw_payload:entity_type::VARCHAR) IN ('customer', 'customers') THEN 'customer'
        END AS entity_type,
        raw_payload:business_key::VARCHAR AS entity_id,
        UPPER(raw_payload:operation::VARCHAR) AS operation,
        TRY_TO_TIMESTAMP_TZ(raw_payload:changed_at::VARCHAR) AS deleted_at,
        raw_payload:reason::VARCHAR AS delete_reason,
        raw_payload:source_lsn::VARCHAR AS source_lsn,
        source_file,
        source_row_number,
        batch_id,
        ingest_ts,
        payload_hash AS record_hash
    FROM source_rows
),

valid AS (
    SELECT
        COALESCE(
            source_lsn,
            record_hash,
            SHA2(CONCAT_WS('|', entity_type, entity_id, deleted_at::VARCHAR), 256)
        ) AS delete_event_id,
        *
    FROM parsed
    WHERE entity_type IN ('order', 'customer')
      AND entity_id IS NOT NULL
      AND operation = 'D'
      AND deleted_at IS NOT NULL
)

SELECT *
FROM valid
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY delete_event_id
    ORDER BY ingest_ts DESC, source_file DESC, source_row_number DESC
) = 1
