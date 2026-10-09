SELECT
    user_id,
    COUNT_IF(is_current) AS current_row_count
FROM {{ ref('dim_customer') }}
GROUP BY user_id
HAVING COUNT_IF(is_current) <> 1
