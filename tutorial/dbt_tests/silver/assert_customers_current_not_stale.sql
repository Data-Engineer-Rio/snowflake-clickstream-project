WITH expected AS (
    SELECT
        user_id,
        updated_at,
        record_hash
    FROM {{ ref('stg_customer_changes') }}
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY user_id
        ORDER BY updated_at DESC, ingest_ts DESC, source_file DESC, source_row_number DESC
    ) = 1
)

SELECT
    expected.user_id,
    expected.updated_at AS expected_updated_at,
    actual.updated_at AS actual_updated_at,
    expected.record_hash AS expected_record_hash,
    actual.record_hash AS actual_record_hash
FROM expected
LEFT JOIN {{ ref('stg_customers_current') }} actual
  ON expected.user_id = actual.user_id
WHERE actual.user_id IS NULL
   OR actual.updated_at <> expected.updated_at
   OR actual.record_hash <> expected.record_hash
