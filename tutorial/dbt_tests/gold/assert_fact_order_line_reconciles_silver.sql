SELECT
    COALESCE(silver.order_id, gold.order_id) AS order_id,
    COALESCE(silver.line_number, gold.line_number) AS line_number,
    silver.line_amount_original AS silver_line_amount_original,
    gold.line_amount_original AS gold_line_amount_original,
    silver.line_amount_usd AS silver_line_amount_usd,
    gold.gross_revenue_usd AS gold_gross_revenue_usd
FROM {{ ref('stg_order_items') }} silver
FULL OUTER JOIN {{ ref('fact_order_line') }} gold
  ON gold.order_id = silver.order_id
 AND gold.line_number = silver.line_number
WHERE silver.order_id IS NULL
   OR gold.order_id IS NULL
   OR ABS(silver.line_amount_original - gold.line_amount_original) > 0.0001
   OR COALESCE(ABS(silver.line_amount_usd - gold.gross_revenue_usd), 0) > 0.0001
   OR (silver.line_amount_usd IS NULL AND gold.gross_revenue_usd IS NOT NULL)
   OR (silver.line_amount_usd IS NOT NULL AND gold.gross_revenue_usd IS NULL)
