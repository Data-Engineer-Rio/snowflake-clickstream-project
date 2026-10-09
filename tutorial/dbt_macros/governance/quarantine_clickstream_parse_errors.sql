{% macro quarantine_clickstream_parse_errors() %}

    {{ log("Quarantining clickstream parse errors", info=true) }}

    {% set quarantine_sql %}
        MERGE INTO ABC_RETAIL.GOVERNANCE.QUARANTINE_RECORDS AS target
        USING (
            SELECT
                SHA2(
                    'clickstream|' ||
                    COALESCE(SOURCE_FILE, '') || '|' ||
                    COALESCE(SOURCE_ROW_NUMBER::VARCHAR, '') ||
                    '|PARSE_ERROR',
                    256
                ) AS QUARANTINE_ID,
                'clickstream' AS SOURCE_NAME,
                SOURCE_FILE,
                SOURCE_ROW_NUMBER,
                BATCH_ID,
                RAW_TEXT,
                RAW_PAYLOAD,
                'PARSE_ERROR' AS ERROR_CODE,
                'TRY_PARSE_JSON returned NULL' AS ERROR_MESSAGE,
                CURRENT_TIMESTAMP() AS QUARANTINED_AT,
                '{{ invocation_id }}' AS RUN_ID
            FROM ABC_RETAIL.BRONZE.BRONZE_CLICKSTREAM
            WHERE NOT IS_PARSE_VALID
        ) AS source
        ON target.QUARANTINE_ID = source.QUARANTINE_ID
        WHEN NOT MATCHED THEN INSERT (
            QUARANTINE_ID,
            SOURCE_NAME,
            SOURCE_FILE,
            SOURCE_ROW_NUMBER,
            BATCH_ID,
            RAW_TEXT,
            RAW_PAYLOAD,
            ERROR_CODE,
            ERROR_MESSAGE,
            QUARANTINED_AT,
            RUN_ID
        ) VALUES (
            source.QUARANTINE_ID,
            source.SOURCE_NAME,
            source.SOURCE_FILE,
            source.SOURCE_ROW_NUMBER,
            source.BATCH_ID,
            source.RAW_TEXT,
            source.RAW_PAYLOAD,
            source.ERROR_CODE,
            source.ERROR_MESSAGE,
            source.QUARANTINED_AT,
            source.RUN_ID
        )
    {% endset %}

    {% do run_query(quarantine_sql) %}

    {% set audit_sql %}
        INSERT INTO ABC_RETAIL.GOVERNANCE.AUDIT_PIPELINE_RUNS (
            RUN_ID,
            PIPELINE_NAME,
            TASK_NAME,
            STARTED_AT,
            ENDED_AT,
            STATUS,
            ROWS_READ,
            ROWS_QUARANTINED,
            SOURCE_FILES,
            QUERY_ID
        )
        SELECT
            '{{ invocation_id }}',
            'bronze_data_quality',
            'quarantine_clickstream_parse_errors',
            '{{ run_started_at }}'::TIMESTAMP_TZ,
            CURRENT_TIMESTAMP(),
            'SUCCESS',
            COUNT(*),
            COUNT_IF(NOT IS_PARSE_VALID),
            ARRAY_AGG(DISTINCT SOURCE_FILE),
            LAST_QUERY_ID()
        FROM ABC_RETAIL.BRONZE.BRONZE_CLICKSTREAM
    {% endset %}

    {% do run_query(audit_sql) %}

    {{ log("Clickstream parse-error quarantine completed", info=true) }}

{% endmacro %}
