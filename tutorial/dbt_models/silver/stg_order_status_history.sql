{{
  config(
    materialized='table',
    tags=['silver', 'history']
  )
}}

SELECT
    change_id AS status_event_id,
    order_id,
    status,
    updated_at AS status_at,
    FALSE AS is_delete_event,
    source_file,
    source_row_number,
    batch_id,
    ingest_ts,
    record_hash
FROM {{ ref('stg_order_changes') }}

UNION ALL

SELECT
    delete_event_id AS status_event_id,
    entity_id AS order_id,
    'deleted' AS status,
    deleted_at AS status_at,
    TRUE AS is_delete_event,
    source_file,
    source_row_number,
    batch_id,
    ingest_ts,
    record_hash
FROM {{ ref('stg_cdc_deletes') }}
WHERE entity_type = 'order'
