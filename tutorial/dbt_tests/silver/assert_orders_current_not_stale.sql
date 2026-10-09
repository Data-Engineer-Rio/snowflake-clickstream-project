WITH expected AS (
    SELECT
        order_id,
        updated_at
    FROM {{ ref('stg_order_changes') }}
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY order_id
        ORDER BY updated_at DESC, ingest_ts DESC, source_file DESC, source_row_number DESC
    ) = 1
)

SELECT
    expected.order_id,
    expected.updated_at AS expected_updated_at,
    actual.updated_at AS actual_updated_at
FROM expected
LEFT JOIN {{ ref('stg_orders_current') }} actual
  ON expected.order_id = actual.order_id
WHERE actual.order_id IS NULL
   OR actual.updated_at <> expected.updated_at
