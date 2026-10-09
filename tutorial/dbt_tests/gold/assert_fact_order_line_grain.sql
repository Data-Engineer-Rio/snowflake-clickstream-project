SELECT
    order_id,
    line_number,
    COUNT(*) AS row_count
FROM {{ ref('fact_order_line') }}
GROUP BY order_id, line_number
HAVING COUNT(*) <> 1
