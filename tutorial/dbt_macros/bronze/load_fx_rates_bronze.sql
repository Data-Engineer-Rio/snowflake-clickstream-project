{% macro load_fx_rates_bronze() %}

    {{ log("FX rates 1/2 - loading CSV file", info=true) }}

    {% set copy_sql %}
        COPY INTO ABC_RETAIL.BRONZE.BRONZE_FX_RATES
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
                    IFNULL($3::VARCHAR, '')
                )::VARCHAR,
                FALSE,
                METADATA$FILENAME,
                METADATA$FILE_ROW_NUMBER,
                METADATA$FILE_LAST_MODIFIED,
                'pending_fx_csv',
                METADATA$START_SCAN_TIME,
                METADATA$FILENAME || ':' ||
                    METADATA$FILE_ROW_NUMBER::VARCHAR
            FROM @ABC_RETAIL.LANDING.STG_S3_LANDING/dim_fx_rates/
            (
                FILE_FORMAT => 'ABC_RETAIL.LANDING.FF_CSV'
            )
        )
        PATTERN = '.*fx_rates[.]csv'
        ON_ERROR = 'CONTINUE'
        FORCE = FALSE
    {% endset %}

    {% do run_query(copy_sql) %}

    {{ log("FX rates 2/2 - building payload and calculating SHA2", info=true) }}

    {% set enrich_sql %}
        UPDATE ABC_RETAIL.BRONZE.BRONZE_FX_RATES
        SET
            RAW_PAYLOAD = OBJECT_CONSTRUCT_KEEP_NULL(
                'rate_date',
                    NULLIF(TRY_PARSE_JSON(RAW_TEXT)[0]::VARCHAR, ''),
                'currency',
                    NULLIF(TRY_PARSE_JSON(RAW_TEXT)[1]::VARCHAR, ''),
                'rate_to_usd',
                    NULLIF(TRY_PARSE_JSON(RAW_TEXT)[2]::VARCHAR, '')
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
                'static_fx'
            ),
            PAYLOAD_HASH = SHA2(RAW_TEXT, 256)
        WHERE BATCH_ID = 'pending_fx_csv'
    {% endset %}

    {% do run_query(enrich_sql) %}

    {{ log("FX rates Bronze ingestion completed", info=true) }}

{% endmacro %}
