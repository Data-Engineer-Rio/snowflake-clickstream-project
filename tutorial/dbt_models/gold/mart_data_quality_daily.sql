{{
  config(
    materialized='table',
    schema='GOLD',
    tags=['gold', 'mart', 'dq']
  )
}}

WITH curated_records AS (
    SELECT TO_DATE(ingest_ts) AS metric_date, 'clickstream' AS source_name
    FROM {{ ref('stg_events') }}

    UNION ALL

    SELECT TO_DATE(ingest_ts), 'orders'
    FROM {{ ref('stg_order_changes') }}

    UNION ALL

    SELECT TO_DATE(ingest_ts), 'customers'
    FROM {{ ref('stg_customer_changes') }}

    UNION ALL

    SELECT TO_DATE(ingest_ts), 'products'
    FROM {{ ref('stg_products') }}

    UNION ALL

    SELECT TO_DATE(ingest_ts), 'fx_rates'
    FROM {{ ref('stg_fx_rates') }}

    UNION ALL

    SELECT TO_DATE(ingest_ts), 'customer_feedback'
    FROM {{ ref('stg_customer_feedback') }}

    UNION ALL

    SELECT TO_DATE(ingest_ts), 'cdc_deletes'
    FROM {{ ref('stg_cdc_deletes') }}
),

valid_metrics AS (
    SELECT
        metric_date,
        source_name,
        'CURATED' AS record_status,
        'VALID' AS error_code,
        COUNT(*) AS record_count
    FROM curated_records
    GROUP BY metric_date, source_name
),

semantic_issue_metrics AS (
    SELECT
        TO_DATE(source_ingest_ts) AS metric_date,
        source_name,
        'QUALITY_ISSUE' AS record_status,
        error_code,
        COUNT(*) AS record_count
    FROM {{ ref('dq_silver_rejections') }}
    GROUP BY TO_DATE(source_ingest_ts), source_name, error_code
),

parse_issue_metrics AS (
    SELECT
        TO_DATE(quarantined_at) AS metric_date,
        CASE source_name
            WHEN 'clickstream_events' THEN 'clickstream'
            WHEN 'customer_feedback' THEN 'customer_feedback'
            ELSE source_name
        END AS source_name,
        'QUALITY_ISSUE' AS record_status,
        error_code,
        COUNT(*) AS record_count
    FROM {{ source('governance', 'quarantine_records') }}
    WHERE error_code = 'PARSE_ERROR'
    GROUP BY
        TO_DATE(quarantined_at),
        CASE source_name
            WHEN 'clickstream_events' THEN 'clickstream'
            WHEN 'customer_feedback' THEN 'customer_feedback'
            ELSE source_name
        END,
        error_code
),

all_metrics AS (
    SELECT * FROM valid_metrics
    UNION ALL
    SELECT * FROM semantic_issue_metrics
    UNION ALL
    SELECT * FROM parse_issue_metrics
)

SELECT
    TO_NUMBER(TO_CHAR(metric_date, 'YYYYMMDD')) AS date_key,
    metric_date,
    source_name,
    record_status,
    error_code,
    SUM(record_count) AS record_count
FROM all_metrics
GROUP BY metric_date, source_name, record_status, error_code
