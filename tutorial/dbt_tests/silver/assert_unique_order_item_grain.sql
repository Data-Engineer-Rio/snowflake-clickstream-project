SELECT
    order_id,
    line_number,
    COUNT(*) AS duplicate_count
FROM {{ ref('stg_order_items') }}
GROUP BY order_id, line_number
HAVING COUNT(*) > 1
