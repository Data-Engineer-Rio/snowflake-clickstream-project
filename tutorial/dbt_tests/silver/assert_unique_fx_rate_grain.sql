SELECT
    rate_date,
    source_currency,
    target_currency,
    COUNT(*) AS duplicate_count
FROM {{ ref('stg_fx_rates') }}
GROUP BY rate_date, source_currency, target_currency
HAVING COUNT(*) > 1
