{{
  config(
    materialized='table',
    schema='GOLD',
    tags=['gold', 'dimension']
  )
}}

WITH country_codes AS (
    SELECT country_code
    FROM {{ ref('stg_order_changes') }}

    UNION

    SELECT country_code
    FROM {{ ref('stg_customer_changes') }}
),

valid_countries AS (
    SELECT DISTINCT country_code
    FROM country_codes
    WHERE country_code IS NOT NULL
      AND TRIM(country_code) <> ''
)

SELECT
    SHA2(CONCAT('GEOGRAPHY|', country_code), 256) AS geography_key,
    country_code,
    FALSE AS is_unknown
FROM valid_countries

UNION ALL

SELECT
    SHA2('GEOGRAPHY|<UNKNOWN>', 256) AS geography_key,
    '<UNKNOWN>' AS country_code,
    TRUE AS is_unknown
