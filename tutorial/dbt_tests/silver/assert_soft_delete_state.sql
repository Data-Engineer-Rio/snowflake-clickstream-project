WITH latest_order_delete AS (
    SELECT entity_id, deleted_at
    FROM {{ ref('stg_cdc_deletes') }}
    WHERE entity_type = 'order'
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY entity_id
        ORDER BY deleted_at DESC, ingest_ts DESC, source_lsn DESC
    ) = 1
),

invalid_orders AS (
    SELECT
        orders.order_id AS entity_id,
        orders.updated_at,
        deletes.deleted_at,
        orders.is_deleted
    FROM {{ ref('stg_orders_current') }} orders
    LEFT JOIN latest_order_delete deletes
      ON orders.order_id = deletes.entity_id
    WHERE orders.is_deleted <> COALESCE(deletes.deleted_at >= orders.updated_at, FALSE)
),

latest_customer_delete AS (
    SELECT entity_id, deleted_at
    FROM {{ ref('stg_cdc_deletes') }}
    WHERE entity_type = 'customer'
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY entity_id
        ORDER BY deleted_at DESC, ingest_ts DESC, source_lsn DESC
    ) = 1
),

invalid_customers AS (
    SELECT
        customers.user_id AS entity_id,
        customers.updated_at,
        deletes.deleted_at,
        customers.is_deleted
    FROM {{ ref('stg_customers_current') }} customers
    LEFT JOIN latest_customer_delete deletes
      ON customers.user_id = deletes.entity_id
    WHERE customers.is_deleted <> COALESCE(deletes.deleted_at >= customers.updated_at, FALSE)
)

SELECT 'order' AS entity_type, * FROM invalid_orders
UNION ALL
SELECT 'customer' AS entity_type, * FROM invalid_customers
