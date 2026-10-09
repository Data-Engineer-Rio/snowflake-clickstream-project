{{
  config(
    materialized='table',
    schema='GOLD',
    tags=['gold', 'mart', 'revenue']
  )
}}

SELECT
    facts.order_date_key AS date_key,
    dates.calendar_date AS report_date,
    geographies.country_code,
    products.category_code,
    products.category_name,
    COUNT(DISTINCT facts.order_id) AS gross_order_count,
    COUNT(DISTINCT IFF(
        facts.order_status NOT IN ('cancelled', 'returned'),
        facts.order_id,
        NULL
    )) AS net_order_count,
    SUM(facts.qty) AS units,
    SUM(COALESCE(facts.gross_revenue_usd, 0))::NUMBER(38, 4) AS gross_revenue_usd,
    SUM(COALESCE(facts.net_revenue_usd, 0))::NUMBER(38, 4) AS net_revenue_usd,
    COUNT_IF(facts.is_fx_rate_missing) AS fx_missing_line_count
FROM {{ ref('fact_order_line') }} facts
INNER JOIN {{ ref('dim_date') }} dates
  ON dates.date_key = facts.order_date_key
INNER JOIN {{ ref('dim_product') }} products
  ON products.product_key = facts.product_key
INNER JOIN {{ ref('dim_geography') }} geographies
  ON geographies.geography_key = facts.geography_key
GROUP BY
    facts.order_date_key,
    dates.calendar_date,
    geographies.country_code,
    products.category_code,
    products.category_name
