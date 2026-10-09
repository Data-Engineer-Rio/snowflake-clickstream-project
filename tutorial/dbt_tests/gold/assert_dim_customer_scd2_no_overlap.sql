SELECT
    earlier.user_id,
    earlier.customer_key AS earlier_customer_key,
    later.customer_key AS later_customer_key
FROM {{ ref('dim_customer') }} earlier
INNER JOIN {{ ref('dim_customer') }} later
  ON later.user_id = earlier.user_id
 AND later.customer_key > earlier.customer_key
 AND earlier.effective_from < COALESCE(later.effective_to, '9999-12-31'::TIMESTAMP_TZ)
 AND later.effective_from < COALESCE(earlier.effective_to, '9999-12-31'::TIMESTAMP_TZ)
WHERE earlier.user_id <> '<UNKNOWN>'
