{{
  config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='event_id',
    on_schema_change='fail',
    tags=['silver']
  )
}}

WITH source_rows AS (
    SELECT *
    FROM {{ source('bronze', 'bronze_clickstream') }}
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
        NULLIF(TRIM(raw_payload:event_id::VARCHAR), '') AS event_id,
        TRY_TO_TIMESTAMP_TZ(raw_payload:event_time::VARCHAR) AS event_time_utc,
        TRY_TO_TIMESTAMP_TZ(raw_payload:ingest_time::VARCHAR) AS ingest_time_utc,
        NULLIF(TRIM(raw_payload:user_id::VARCHAR), '') AS user_id,
        NULLIF(TRIM(raw_payload:session_id::VARCHAR), '') AS session_id,
        COALESCE(
            NULLIF(TRIM(raw_payload:user_id::VARCHAR), ''),
            'anonymous:' || NULLIF(TRIM(raw_payload:session_id::VARCHAR), '')
        ) AS analytic_user_id,
        LOWER(TRIM(raw_payload:event_type::VARCHAR)) AS event_type,
        raw_payload:page::VARCHAR AS page,
        CASE
            WHEN TYPEOF(raw_payload:device) = 'OBJECT' THEN raw_payload:device:os::VARCHAR
            ELSE SPLIT_PART(raw_payload:device::VARCHAR, '|', 1)
        END AS device_os,
        CASE
            WHEN TYPEOF(raw_payload:device) = 'OBJECT' THEN raw_payload:device:model::VARCHAR
            ELSE SPLIT_PART(raw_payload:device::VARCHAR, '|', 2)
        END AS device_model,
        raw_payload:attributes AS attributes,
        source_file,
        source_row_number,
        batch_id,
        ingest_ts,
        COALESCE(TRY_TO_TIMESTAMP_TZ(raw_payload:ingest_time::VARCHAR), ingest_ts) AS event_version_ts,
        payload_hash AS record_hash
    FROM source_rows
),

valid AS (
    SELECT *
    FROM parsed
    WHERE event_id IS NOT NULL
      AND event_time_utc IS NOT NULL
      AND analytic_user_id IS NOT NULL
      AND event_type IN ('view', 'add_to_cart', 'checkout', 'purchase')
),

source_winners AS (
    SELECT *
    FROM valid
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY event_id
        ORDER BY event_version_ts DESC, ingest_ts DESC, source_file DESC, source_row_number DESC
    ) = 1
)

{% if is_incremental() %}
SELECT candidate.*
FROM source_winners candidate
LEFT JOIN {{ this }} target
  ON candidate.event_id = target.event_id
WHERE target.event_id IS NULL
   OR candidate.event_version_ts > target.event_version_ts
   OR (
       candidate.event_version_ts = target.event_version_ts
       AND candidate.ingest_ts > target.ingest_ts
   )
   OR (
       candidate.event_version_ts = target.event_version_ts
       AND candidate.ingest_ts = target.ingest_ts
       AND CONCAT(candidate.source_file, '|', LPAD(candidate.source_row_number::VARCHAR, 20, '0'))
           > CONCAT(target.source_file, '|', LPAD(target.source_row_number::VARCHAR, 20, '0'))
   )
{% else %}
SELECT * FROM source_winners
{% endif %}
