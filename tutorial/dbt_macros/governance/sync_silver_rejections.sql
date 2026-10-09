{% macro sync_silver_rejections() %}
    {{ log("Syncing semantic Silver rejections to GOVERNANCE.QUARANTINE_RECORDS", info=true) }}

    {% set merge_sql %}
        MERGE INTO ABC_RETAIL.GOVERNANCE.QUARANTINE_RECORDS AS target
        USING (
            SELECT
                rejection_id AS quarantine_id,
                source_name,
                source_file,
                source_row_number,
                batch_id,
                raw_text,
                raw_payload,
                error_code,
                error_message,
                'silver:' || '{{ invocation_id }}' AS run_id
            FROM {{ ref('dq_silver_rejections') }}
        ) AS source
        ON target.quarantine_id = source.quarantine_id
        WHEN MATCHED THEN UPDATE SET
            target.error_message = source.error_message,
            target.run_id = source.run_id
        WHEN NOT MATCHED THEN INSERT (
            quarantine_id,
            source_name,
            source_file,
            source_row_number,
            batch_id,
            raw_text,
            raw_payload,
            error_code,
            error_message,
            quarantined_at,
            run_id
        ) VALUES (
            source.quarantine_id,
            source.source_name,
            source.source_file,
            source.source_row_number,
            source.batch_id,
            source.raw_text,
            source.raw_payload,
            source.error_code,
            source.error_message,
            CURRENT_TIMESTAMP(),
            source.run_id
        )
    {% endset %}

    {% do run_query(merge_sql) %}
    {{ log("Silver semantic quarantine sync completed", info=true) }}
{% endmacro %}
