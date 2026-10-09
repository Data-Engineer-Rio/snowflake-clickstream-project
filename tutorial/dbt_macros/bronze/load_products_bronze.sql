{% macro load_products_bronze() %}

    {{ log("Products 1/2 - loading CSV files", info=true) }}

    {% set copy_sql %}
        COPY INTO ABC_RETAIL.BRONZE.BRONZE_PRODUCTS
        (
            RAW_TEXT,
            IS_PARSE_VALID,
            SOURCE_FILE,
            SOURCE_ROW_NUMBER,
            FILE_LAST_MODIFIED,
            BATCH_ID,
            INGEST_TS,
            PAYLOAD_HASH
        )
        FROM
        (
            SELECT
                ARRAY_CONSTRUCT(
                    IFNULL($1::VARCHAR, ''),
                    IFNULL($2::VARCHAR, ''),
                    IFNULL($3::VARCHAR, ''),
                    IFNULL($4::VARCHAR, ''),
                    IFNULL($5::VARCHAR, ''),
                    IFNULL($6::VARCHAR, ''),
                    IFNULL($7::VARCHAR, ''),
                    IFNULL($8::VARCHAR, ''),
                    IFNULL($9::VARCHAR, '')
                )::VARCHAR,
                FALSE,
                METADATA$FILENAME,
                METADATA$FILE_ROW_NUMBER,
                METADATA$FILE_LAST_MODIFIED,
                'pending_products_csv',
                METADATA$START_SCAN_TIME,
                METADATA$FILENAME || ':' ||
                    METADATA$FILE_ROW_NUMBER::VARCHAR
            FROM @ABC_RETAIL.LANDING.STG_S3_LANDING/products/
            (
                FILE_FORMAT => 'ABC_RETAIL.LANDING.FF_CSV'
            )
        )
        PATTERN = '.*products[.]csv'
        ON_ERROR = 'CONTINUE'
        FORCE = FALSE
    {% endset %}

    {% do run_query(copy_sql) %}

    {{ log("Products 2/2 - building payload and calculating SHA2", info=true) }}

    {% set enrich_sql %}
        UPDATE ABC_RETAIL.BRONZE.BRONZE_PRODUCTS
        SET
            RAW_PAYLOAD = OBJECT_CONSTRUCT_KEEP_NULL(
                'sku',
                    NULLIF(TRY_PARSE_JSON(RAW_TEXT)[0]::VARCHAR, ''),
                'product_name',
                    NULLIF(TRY_PARSE_JSON(RAW_TEXT)[1]::VARCHAR, ''),
                'brand',
                    NULLIF(TRY_PARSE_JSON(RAW_TEXT)[2]::VARCHAR, ''),
                'subcategory_code',
                    NULLIF(TRY_PARSE_JSON(RAW_TEXT)[3]::VARCHAR, ''),
                'subcategory_name',
                    NULLIF(TRY_PARSE_JSON(RAW_TEXT)[4]::VARCHAR, ''),
                'category_code',
                    NULLIF(TRY_PARSE_JSON(RAW_TEXT)[5]::VARCHAR, ''),
                'category_name',
                    NULLIF(TRY_PARSE_JSON(RAW_TEXT)[6]::VARCHAR, ''),
                'unit_cost',
                    NULLIF(TRY_PARSE_JSON(RAW_TEXT)[7]::VARCHAR, ''),
                'updated_at',
                    NULLIF(TRY_PARSE_JSON(RAW_TEXT)[8]::VARCHAR, '')
            ),
            IS_PARSE_VALID = TRY_PARSE_JSON(RAW_TEXT) IS NOT NULL,
            BATCH_ID = COALESCE(
                REGEXP_SUBSTR(
                    SOURCE_FILE,
                    'batch_id=([^/]+)',
                    1,
                    1,
                    'e',
                    1
                ),
                'unknown'
            ),
            PAYLOAD_HASH = SHA2(RAW_TEXT, 256)
        WHERE BATCH_ID = 'pending_products_csv'
    {% endset %}

    {% do run_query(enrich_sql) %}

    {{ log("Products Bronze ingestion completed", info=true) }}

{% endmacro %}
