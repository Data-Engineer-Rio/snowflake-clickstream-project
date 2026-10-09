{{
  config(
    materialized='table',
    schema='GOLD',
    tags=['gold', 'dimension', 'scd2']
  )
}}

WITH upsert_events AS (
    SELECT
        user_id,
        email,
        phone,
        country_code,
        created_at,
        updated_at AS effective_from,
        FALSE AS is_deleted,
        NULL::TIMESTAMP_TZ AS deleted_at,
        NULL::VARCHAR AS delete_reason,
        record_hash AS state_hash,
        ingest_ts,
        source_file,
        source_row_number,
        1 AS event_priority
    FROM {{ ref('stg_customer_changes') }}
),

delete_events AS (
    SELECT
        deletes.entity_id AS user_id,
        changes.email,
        changes.phone,
        changes.country_code,
        changes.created_at,
        deletes.deleted_at AS effective_from,
        TRUE AS is_deleted,
        deletes.deleted_at,
        deletes.delete_reason,
        SHA2(
            CONCAT_WS('|', 'DELETE', deletes.entity_id, deletes.deleted_at::VARCHAR),
            256
        ) AS state_hash,
        deletes.ingest_ts,
        deletes.source_file,
        deletes.source_row_number,
        2 AS event_priority
    FROM {{ ref('stg_cdc_deletes') }} deletes
    LEFT JOIN {{ ref('stg_customer_changes') }} changes
      ON changes.user_id = deletes.entity_id
     AND changes.updated_at <= deletes.deleted_at
    WHERE deletes.entity_type = 'customer'
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY deletes.delete_event_id
        ORDER BY changes.updated_at DESC,
                 changes.ingest_ts DESC,
                 changes.source_file DESC,
                 changes.source_row_number DESC
    ) = 1
),

all_state_events AS (
    SELECT * FROM upsert_events
    UNION ALL
    SELECT * FROM delete_events
),

same_timestamp_winners AS (
    SELECT *
    FROM all_state_events
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY user_id, effective_from
        ORDER BY event_priority DESC,
                 ingest_ts DESC,
                 source_file DESC,
                 source_row_number DESC
    ) = 1
),

with_previous_state AS (
    SELECT
        *,
        LAG(state_hash) OVER (
            PARTITION BY user_id
            ORDER BY effective_from, event_priority, ingest_ts
        ) AS previous_state_hash
    FROM same_timestamp_winners
),

state_transitions AS (
    SELECT *
    FROM with_previous_state
    WHERE previous_state_hash IS NULL
       OR previous_state_hash <> state_hash
),

timeline AS (
    SELECT
        *,
        LEAD(effective_from) OVER (
            PARTITION BY user_id
            ORDER BY effective_from, event_priority, ingest_ts
        ) AS effective_to
    FROM state_transitions
),

known_members AS (
    SELECT
        SHA2(
            CONCAT_WS('|', 'CUSTOMER', user_id, effective_from::VARCHAR, state_hash),
            256
        ) AS customer_key,
        user_id,
        email,
        phone,
        country_code,
        created_at,
        effective_from,
        effective_to,
        effective_to IS NULL AS is_current,
        is_deleted,
        deleted_at,
        delete_reason,
        state_hash AS record_hash,
        ingest_ts AS source_ingest_ts,
        source_file,
        source_row_number
    FROM timeline
)

SELECT * FROM known_members

UNION ALL

SELECT
    SHA2('CUSTOMER|<UNKNOWN>', 256) AS customer_key,
    '<UNKNOWN>' AS user_id,
    NULL::VARCHAR AS email,
    NULL::VARCHAR AS phone,
    '<UNKNOWN>' AS country_code,
    NULL::TIMESTAMP_TZ AS created_at,
    '1900-01-01'::TIMESTAMP_TZ AS effective_from,
    NULL::TIMESTAMP_TZ AS effective_to,
    TRUE AS is_current,
    FALSE AS is_deleted,
    NULL::TIMESTAMP_TZ AS deleted_at,
    NULL::VARCHAR AS delete_reason,
    SHA2('CUSTOMER|<UNKNOWN>', 256) AS record_hash,
    NULL::TIMESTAMP_TZ AS source_ingest_ts,
    NULL::VARCHAR AS source_file,
    NULL::NUMBER AS source_row_number
