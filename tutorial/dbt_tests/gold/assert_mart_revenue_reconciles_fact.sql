WITH fact_totals AS (
    SELECT
        order_date_key AS date_key,
        SUM(COALESCE(gross_revenue_usd, 0))::NUMBER(38, 4) AS gross_revenue_usd,
        SUM(COALESCE(net_revenue_usd, 0))::NUMBER(38, 4) AS net_revenue_usd
    FROM {{ ref('fact_order_line') }}
    GROUP BY order_date_key
),

mart_totals AS (
    SELECT
        date_key,
        SUM(gross_revenue_usd)::NUMBER(38, 4) AS gross_revenue_usd,
        SUM(net_revenue_usd)::NUMBER(38, 4) AS net_revenue_usd
    FROM {{ ref('mart_revenue_daily') }}
    GROUP BY date_key
)

SELECT
    COALESCE(facts.date_key, marts.date_key) AS date_key,
    facts.gross_revenue_usd AS fact_gross_revenue_usd,
    marts.gross_revenue_usd AS mart_gross_revenue_usd,
    facts.net_revenue_usd AS fact_net_revenue_usd,
    marts.net_revenue_usd AS mart_net_revenue_usd
FROM fact_totals facts
FULL OUTER JOIN mart_totals marts
  ON marts.date_key = facts.date_key
WHERE ABS(COALESCE(facts.gross_revenue_usd, 0) - COALESCE(marts.gross_revenue_usd, 0)) > 0.01
   OR ABS(COALESCE(facts.net_revenue_usd, 0) - COALESCE(marts.net_revenue_usd, 0)) > 0.01
