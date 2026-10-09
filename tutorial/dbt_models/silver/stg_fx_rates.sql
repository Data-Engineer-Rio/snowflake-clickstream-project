{{
  config(
    materialized='table',
    tags=['silver', 'reference']
  )
}}

WITH parsed AS (
    SELECT
        TRY_TO_DATE(raw_payload:rate_date::VARCHAR) AS rate_date,
        NULLIF(UPPER(TRIM(raw_payload:currency::VARCHAR)), '') AS source_currency,
        'USD' AS target_currency,
        TRY_TO_DOUBLE(raw_payload:rate_to_usd::VARCHAR)::NUMBER(18, 8) AS rate,
        source_file,
        source_row_number,
        batch_id,
        ingest_ts
    FROM {{ source('bronze', 'bronze_fx_rates') }}
    WHERE is_parse_valid
)

SELECT *
FROM parsed
WHERE rate_date IS NOT NULL
  AND source_currency IS NOT NULL
  AND rate > 0
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY rate_date, source_currency, target_currency
    ORDER BY ingest_ts DESC, source_file DESC, source_row_number DESC
) = 1
