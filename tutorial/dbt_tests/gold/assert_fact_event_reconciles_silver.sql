SELECT
    COALESCE(silver.event_id, gold.event_id) AS event_id
FROM {{ ref('stg_events') }} silver
FULL OUTER JOIN {{ ref('fact_customer_event') }} gold
  ON gold.event_id = silver.event_id
WHERE silver.event_id IS NULL
   OR gold.event_id IS NULL
