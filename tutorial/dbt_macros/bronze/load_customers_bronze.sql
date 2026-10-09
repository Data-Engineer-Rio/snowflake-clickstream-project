{% macro load_customers_bronze() %}

    {{ log("Customers 1/4 - loading JSON files", info=true) }}

    {% set copy_json_sql %}
        COPY INTO ABC_RETAIL.BRONZE.BRONZE_CUSTOMERS
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
                'pending_customers_json',
                METADATA$START_SCAN_TIME,
                METADATA$FILENAME || ':' ||
                    METADATA$FILE_ROW_NUMBER::VARCHAR
            FROM @ABC_RETAIL.LANDING.STG_S3_LANDING/customers/
            (
                FILE_FORMAT => 'ABC_RETAIL.LANDING.FF_RAW_LINE'
            )
        )
        PATTERN = '.*customers[.]json'
        ON_ERROR = 'CONTINUE'
        FORCE = FALSE
    {% endset %}

    {% do run_query(copy_json_sql) %}

    {{ log("Customers 2/4 - loading CSV files", info=true) }}

    {% set copy_csv_sql %}
        COPY INTO ABC_RETAIL.BRONZE.BRONZE_CUSTOMERS
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
                    IFNULL($6::VARCHAR, '')
                )::VARCHAR,
                FALSE,
                METADATA$FILENAME,
                METADATA$FILE_ROW_NUMBER,
                METADATA$FILE_LAST_MODIFIED,
                'pending_customers_csv',
                METADATA$START_SCAN_TIME,
                METADATA$FILENAME || ':' ||
                    METADATA$FILE_ROW_NUMBER::VARCHAR
            FROM @ABC_RETAIL.LANDING.STG_S3_LANDING/customers/
            (
                FILE_FORMAT => 'ABC_RETAIL.LANDING.FF_CSV'
            )
        )
        PATTERN = '.*customers[.]csv'
        ON_ERROR = 'CONTINUE'
        FORCE = FALSE
    {% endset %}

    {% do run_query(copy_csv_sql) %}

    {{ log("Customers 3/4 - parsing JSON rows", info=true) }}

    {% set enrich_json_sql %}
        UPDATE ABC_RETAIL.BRONZE.BRONZE_CUSTOMERS
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
        WHERE BATCH_ID = 'pending_customers_json'
    {% endset %}

    {% do run_query(enrich_json_sql) %}

    {{ log("Customers 4/4 - building payload for CSV rows", info=true) }}

    {% set enrich_csv_sql %}
        UPDATE ABC_RETAIL.BRONZE.BRONZE_CUSTOMERS
        SET
            RAW_PAYLOAD = OBJECT_CONSTRUCT_KEEP_NULL(
                'user_id',
                    NULLIF(TRY_PARSE_JSON(RAW_TEXT)[0]::VARCHAR, ''),
                'email',
                    NULLIF(TRY_PARSE_JSON(RAW_TEXT)[1]::VARCHAR, ''),
                'phone',
                    NULLIF(TRY_PARSE_JSON(RAW_TEXT)[2]::VARCHAR, ''),
                'country',
                    NULLIF(TRY_PARSE_JSON(RAW_TEXT)[3]::VARCHAR, ''),
                'created_at',
                    NULLIF(TRY_PARSE_JSON(RAW_TEXT)[4]::VARCHAR, ''),
                'updated_at',
                    NULLIF(TRY_PARSE_JSON(RAW_TEXT)[5]::VARCHAR, '')
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
        WHERE BATCH_ID = 'pending_customers_csv'
    {% endset %}

    {% do run_query(enrich_csv_sql) %}

    {{ log("Customers Bronze ingestion completed", info=true) }}

{% endmacro %}
