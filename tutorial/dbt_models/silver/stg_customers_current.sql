{{
  config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='user_id',
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
    SELECT user_id
    FROM {{ ref('stg_customer_changes') }}, watermark
    WHERE ingest_ts >= watermark.min_ingest_ts

    UNION

    SELECT entity_id AS user_id
    FROM {{ ref('stg_cdc_deletes') }}, watermark
    WHERE entity_type = 'customer'
      AND ingest_ts >= watermark.min_ingest_ts
),
{% else %}
WITH impacted_keys AS (
    SELECT DISTINCT user_id
    FROM {{ ref('stg_customer_changes') }}
),
{% endif %}

latest_change AS (
    SELECT changes.*
    FROM {{ ref('stg_customer_changes') }} changes
    INNER JOIN impacted_keys keys
      ON changes.user_id = keys.user_id
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY changes.user_id
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
      ON deletes.entity_id = keys.user_id
    WHERE deletes.entity_type = 'customer'
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY deletes.entity_id
        ORDER BY deletes.deleted_at DESC, deletes.ingest_ts DESC, deletes.source_lsn DESC
    ) = 1
)

SELECT
    changes.user_id,
    changes.email,
    changes.phone,
    changes.country_code,
    changes.created_at,
    changes.updated_at,
    IFF(deletes.deleted_at >= changes.updated_at, TRUE, FALSE) AS is_deleted,
    IFF(deletes.deleted_at >= changes.updated_at, deletes.deleted_at, NULL) AS deleted_at,
    IFF(deletes.deleted_at >= changes.updated_at, deletes.delete_reason, NULL) AS delete_reason,
    IFF(deletes.deleted_at >= changes.updated_at, deletes.source_lsn, NULL) AS delete_source_lsn,
    changes.record_hash,
    changes.source_file,
    changes.source_row_number,
    changes.batch_id,
    changes.ingest_ts,
    GREATEST(changes.ingest_ts, COALESCE(deletes.ingest_ts, changes.ingest_ts)) AS state_updated_ingest_ts
FROM latest_change changes
LEFT JOIN latest_delete deletes
  ON changes.user_id = deletes.entity_id
