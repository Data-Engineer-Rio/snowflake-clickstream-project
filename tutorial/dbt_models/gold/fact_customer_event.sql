{{
  config(
    materialized='table',
    schema='GOLD',
    tags=['gold', 'fact']
  )
}}

SELECT
    events.event_id AS customer_event_key,
    events.event_id,
    TO_NUMBER(TO_CHAR(TO_DATE(events.event_time_utc), 'YYYYMMDD')) AS event_date_key,
    COALESCE(customers.customer_key, SHA2('CUSTOMER|<UNKNOWN>', 256)) AS customer_key,
    events.user_id,
    events.analytic_user_id,
    events.session_id,
    events.event_type,
    events.event_time_utc,
    events.page,
    COALESCE(NULLIF(LOWER(TRIM(events.device_os)), ''), '<unknown>') AS device_os,
    COALESCE(NULLIF(LOWER(TRIM(events.device_model)), ''), '<unknown>') AS device_model,
    events.attributes,
    events.source_file,
    events.source_row_number,
    events.batch_id,
    events.ingest_ts,
    events.record_hash
FROM {{ ref('stg_events') }} events
LEFT JOIN {{ ref('dim_customer') }} customers
  ON customers.user_id = events.user_id
 AND events.event_time_utc >= customers.effective_from
 AND events.event_time_utc < COALESCE(customers.effective_to, '9999-12-31'::TIMESTAMP_TZ)
 AND customers.user_id <> '<UNKNOWN>'
