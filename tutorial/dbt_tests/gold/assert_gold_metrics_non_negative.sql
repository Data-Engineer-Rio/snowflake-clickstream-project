SELECT
    'mart_revenue_daily' AS model_name,
    CONCAT_WS('|', date_key, country_code, category_code) AS metric_key
FROM {{ ref('mart_revenue_daily') }}
WHERE gross_order_count < 0
   OR net_order_count < 0
   OR units < 0
   OR gross_revenue_usd < 0
   OR net_revenue_usd < 0
   OR fx_missing_line_count < 0

UNION ALL

SELECT
    'mart_data_quality_daily' AS model_name,
    CONCAT_WS('|', date_key, source_name, record_status, error_code) AS metric_key
FROM {{ ref('mart_data_quality_daily') }}
WHERE record_count < 0
