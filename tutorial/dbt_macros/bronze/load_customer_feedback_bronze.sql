{% macro load_customer_feedback_bronze() %}

    {{ log("Customer feedback 1/2 - loading raw JSON lines", info=true) }}

    {% set copy_sql %}
        COPY INTO ABC_RETAIL.BRONZE.BRONZE_CUSTOMER_FEEDBACK
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
                'pending_feedback_json',
                METADATA$START_SCAN_TIME,
                METADATA$FILENAME || ':' ||
                    METADATA$FILE_ROW_NUMBER::VARCHAR
            FROM @ABC_RETAIL.LANDING.STG_S3_LANDING/customer_feedback/
            (
                FILE_FORMAT => 'ABC_RETAIL.LANDING.FF_RAW_LINE'
            )
        )
        PATTERN = '.*feedback[.]json'
        ON_ERROR = 'CONTINUE'
        FORCE = FALSE
    {% endset %}

    {% do run_query(copy_sql) %}

    {{ log("Customer feedback 2/2 - parsing and calculating SHA2", info=true) }}

    {% set enrich_sql %}
        UPDATE ABC_RETAIL.BRONZE.BRONZE_CUSTOMER_FEEDBACK
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
        WHERE BATCH_ID = 'pending_feedback_json'
    {% endset %}

    {% do run_query(enrich_sql) %}

    {{ log("Customer feedback Bronze ingestion completed", info=true) }}

{% endmacro %}
