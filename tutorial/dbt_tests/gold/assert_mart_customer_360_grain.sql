SELECT
    user_id,
    COUNT(*) AS row_count
FROM {{ ref('mart_customer_360') }}
GROUP BY user_id
HAVING COUNT(*) <> 1
