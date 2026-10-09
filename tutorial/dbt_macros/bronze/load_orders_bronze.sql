{% macro load_orders_bronze() %}

    {{ log("Orders 1/3 - loading JSON files", info=true) }}

    {% set copy_json_sql %}
        COPY INTO ABC_RETAIL.BRONZE.BRONZE_ORDERS
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
                $1::VARCHAR,
                FALSE,
                METADATA$FILENAME,
                METADATA$FILE_ROW_NUMBER,
                METADATA$FILE_LAST_MODIFIED,
                'pending_orders_json',
                METADATA$START_SCAN_TIME,
                METADATA$FILENAME || ':' ||
                    METADATA$FILE_ROW_NUMBER::VARCHAR
            FROM @ABC_RETAIL.LANDING.STG_S3_LANDING/orders/
            (
                FILE_FORMAT => 'ABC_RETAIL.LANDING.FF_RAW_LINE'
            )
        )
        PATTERN = '.*orders[.]json'
        ON_ERROR = 'CONTINUE'
        FORCE = FALSE
    {% endset %}

    {% do run_query(copy_json_sql) %}

    {{ log("Orders 2/3 - loading Parquet files", info=true) }}

    {% set copy_parquet_sql %}
        COPY INTO ABC_RETAIL.BRONZE.BRONZE_ORDERS
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
                $1::VARCHAR,
                FALSE,
                METADATA$FILENAME,
                METADATA$FILE_ROW_NUMBER,
                METADATA$FILE_LAST_MODIFIED,
                'pending_orders_parquet',
                METADATA$START_SCAN_TIME,
                METADATA$FILENAME || ':' ||
                    METADATA$FILE_ROW_NUMBER::VARCHAR
            FROM @ABC_RETAIL.LANDING.STG_S3_LANDING/orders/
            (
                FILE_FORMAT => 'ABC_RETAIL.LANDING.FF_PARQUET'
            )
        )
        PATTERN = '.*orders[.]parquet'
        ON_ERROR = 'CONTINUE'
        FORCE = FALSE
    {% endset %}

    {% do run_query(copy_parquet_sql) %}

    {{ log("Orders 3/3 - parsing payload and calculating SHA2", info=true) }}

    {% set enrich_sql %}
        UPDATE ABC_RETAIL.BRONZE.BRONZE_ORDERS
        SET
            RAW_PAYLOAD = TRY_PARSE_JSON(RAW_TEXT),
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
        WHERE BATCH_ID IN (
            'pending_orders_json',
            'pending_orders_parquet'
        )
    {% endset %}

    {% do run_query(enrich_sql) %}

    {{ log("Orders Bronze ingestion completed", info=true) }}

{% endmacro %}
