WITH valid_source AS (
    SELECT DISTINCT raw_payload:source_lsn::VARCHAR AS source_lsn
    FROM {{ source('bronze', 'bronze_cdc_deletes') }}
    WHERE is_parse_valid
      AND LOWER(raw_payload:entity_type::VARCHAR) IN ('order', 'orders', 'customer', 'customers')
      AND NULLIF(TRIM(raw_payload:business_key::VARCHAR), '') IS NOT NULL
      AND UPPER(raw_payload:operation::VARCHAR) = 'D'
      AND TRY_TO_TIMESTAMP_TZ(raw_payload:changed_at::VARCHAR) IS NOT NULL
)

SELECT source.source_lsn
FROM valid_source source
LEFT JOIN {{ ref('stg_cdc_deletes') }} silver
  ON source.source_lsn = silver.source_lsn
WHERE silver.source_lsn IS NULL
