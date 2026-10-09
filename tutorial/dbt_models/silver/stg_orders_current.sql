{{
  config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='order_id',
    on_schema_change='fail',
    tags=['silver', 'current']
  )
}}

{% if is_incremental() %}
WITH watermark AS (
    SELECT DATEADD(
        DAY,
        -{{ var('silver_lookback_days', 7) }},
        COALESCE(MAX(state_updated_ingest_ts), '1900-01-01'::TIMESTAMP_TZ)
    ) AS min_ingest_ts
    FROM {{ this }}
),

impacted_keys AS (
    SELECT order_id
    FROM {{ ref('stg_order_changes') }}, watermark
    WHERE ingest_ts >= watermark.min_ingest_ts

    UNION

    SELECT entity_id AS order_id
    FROM {{ ref('stg_cdc_deletes') }}, watermark
    WHERE entity_type = 'order'
      AND ingest_ts >= watermark.min_ingest_ts
),
{% else %}
WITH impacted_keys AS (
    SELECT DISTINCT order_id
    FROM {{ ref('stg_order_changes') }}
),
{% endif %}

latest_change AS (
    SELECT changes.*
    FROM {{ ref('stg_order_changes') }} changes
    INNER JOIN impacted_keys keys
      ON changes.order_id = keys.order_id
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY changes.order_id
        ORDER BY changes.updated_at DESC,
                 changes.ingest_ts DESC,
                 changes.source_file DESC,
                 changes.source_row_number DESC
    ) = 1
),

latest_delete AS (
    SELECT deletes.*
    FROM {{ ref('stg_cdc_deletes') }} deletes
    INNER JOIN impacted_keys keys
      ON deletes.entity_id = keys.order_id
    WHERE deletes.entity_type = 'order'
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY deletes.entity_id
        ORDER BY deletes.deleted_at DESC, deletes.ingest_ts DESC, deletes.source_lsn DESC
    ) = 1
)

SELECT
    changes.order_id,
    changes.order_time_utc,
    changes.user_id,
    changes.currency,
    changes.status,
    changes.country_code,
    changes.updated_at,
    changes.items,
    IFF(deletes.deleted_at >= changes.updated_at, TRUE, FALSE) AS is_deleted,
    IFF(deletes.deleted_at >= changes.updated_at, deletes.deleted_at, NULL) AS deleted_at,
    IFF(deletes.deleted_at >= changes.updated_at, deletes.delete_reason, NULL) AS delete_reason,
    IFF(deletes.deleted_at >= changes.updated_at, deletes.source_lsn, NULL) AS delete_source_lsn,
    changes.source_file,
    changes.source_row_number,
    changes.batch_id,
    changes.ingest_ts,
    GREATEST(changes.ingest_ts, COALESCE(deletes.ingest_ts, changes.ingest_ts)) AS state_updated_ingest_ts,
    changes.record_hash
FROM latest_change changes
LEFT JOIN latest_delete deletes
  ON changes.order_id = deletes.entity_id
