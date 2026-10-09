USE ROLE ABC_ENGINEER;
USE WAREHOUSE ABC_TRANSFORM_WH;
USE DATABASE ABC_RETAIL;
USE SCHEMA GOVERNANCE;

CREATE TABLE IF NOT EXISTS QUARANTINE_RECORDS (
    QUARANTINE_ID VARCHAR NOT NULL,
    SOURCE_NAME VARCHAR NOT NULL,
    SOURCE_FILE VARCHAR,
    SOURCE_ROW_NUMBER NUMBER,
    BATCH_ID VARCHAR,
    RAW_TEXT VARCHAR,
    RAW_PAYLOAD VARIANT,
    ERROR_CODE VARCHAR NOT NULL,
    ERROR_MESSAGE VARCHAR,
    QUARANTINED_AT TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP(),
    RUN_ID VARCHAR,
    CONSTRAINT PK_QUARANTINE_RECORDS PRIMARY KEY (QUARANTINE_ID)
);

CREATE TABLE IF NOT EXISTS AUDIT_PIPELINE_RUNS (
    RUN_ID VARCHAR NOT NULL,
    PIPELINE_NAME VARCHAR NOT NULL,
    TASK_NAME VARCHAR NOT NULL,
    BATCH_ID VARCHAR,
    STARTED_AT TIMESTAMP_TZ,
    ENDED_AT TIMESTAMP_TZ,
    STATUS VARCHAR,
    ROWS_READ NUMBER DEFAULT 0,
    ROWS_INSERTED NUMBER DEFAULT 0,
    ROWS_UPDATED NUMBER DEFAULT 0,
    ROWS_DELETED NUMBER DEFAULT 0,
    ROWS_QUARANTINED NUMBER DEFAULT 0,
    SOURCE_FILES ARRAY,
    QUERY_ID VARCHAR,
    ERROR_MESSAGE VARCHAR,
    CONSTRAINT PK_AUDIT_PIPELINE_RUNS PRIMARY KEY (RUN_ID, TASK_NAME)
);

CREATE TABLE IF NOT EXISTS DQ_RULES (
    RULE_CODE VARCHAR NOT NULL,
    ENTITY_NAME VARCHAR NOT NULL,
    RULE_DESCRIPTION VARCHAR,
    SEVERITY VARCHAR,
    IS_ACTIVE BOOLEAN,
    CONSTRAINT PK_DQ_RULES PRIMARY KEY (RULE_CODE, ENTITY_NAME)
);

MERGE INTO DQ_RULES AS target
USING (
    SELECT
        COLUMN1::VARCHAR AS RULE_CODE,
        COLUMN2::VARCHAR AS ENTITY_NAME,
        COLUMN3::VARCHAR AS RULE_DESCRIPTION,
        COLUMN4::VARCHAR AS SEVERITY,
        COLUMN5::BOOLEAN AS IS_ACTIVE
    FROM VALUES
        ('PARSE_ERROR',       'ALL',        'Raw payload cannot be parsed',       'ERROR', TRUE),
        ('MISSING_KEY',       'ALL',        'Business key is null',               'ERROR', TRUE),
        ('INVALID_TIMESTAMP', 'ALL',        'Timestamp cannot be parsed',          'ERROR', TRUE),
        ('INVALID_QUANTITY',  'ORDER_ITEM', 'Quantity must be greater than zero', 'ERROR', TRUE),
        ('INVALID_EMAIL',     'CUSTOMER',   'Email format is invalid',             'WARN',  TRUE),
        ('FX_RATE_MISSING',   'ORDER',      'FX rate is unavailable',              'ERROR', TRUE),
        ('SCD_OVERLAP',       'CUSTOMER',   'SCD validity ranges overlap',         'ERROR', TRUE),
        ('INVALID_EVENT_TYPE','EVENT',      'Event type is unsupported',           'ERROR', TRUE),
        ('MISSING_ANALYTIC_USER','EVENT',   'Both user_id and session_id are null','ERROR', TRUE),
        ('INVALID_STATUS',    'ORDER',      'Order status is unsupported',          'ERROR', TRUE),
        ('MISSING_CURRENCY',  'ORDER',      'Order currency is missing',            'ERROR', TRUE),
        ('INVALID_ITEMS',     'ORDER',      'Order items is not an array',          'ERROR', TRUE),
        ('INVALID_ORDER_ITEM','ORDER_ITEM', 'SKU, quantity or price is invalid',    'ERROR', TRUE),
        ('UNKNOWN_SKU',       'ORDER_ITEM', 'SKU does not exist in product master', 'ERROR', TRUE),
        ('MISSING_CONTACT',   'CUSTOMER',   'Both email and phone are missing',     'WARN',  TRUE),
        ('INVALID_PRODUCT',   'PRODUCT',    'Required product attributes are invalid','ERROR', TRUE),
        ('INVALID_FX_RATE',   'FX_RATE',    'FX rate is missing or non-positive',   'ERROR', TRUE),
        ('EMPTY_TEXT',        'FEEDBACK',   'Feedback text is empty',               'ERROR', TRUE),
        ('PII_DETECTED',      'FEEDBACK',   'Feedback contains detectable PII',     'WARN',  TRUE),
        ('INVALID_CDC_DELETE','CDC_DELETE', 'CDC delete payload is invalid',        'ERROR', TRUE)
) AS source
ON  target.RULE_CODE = source.RULE_CODE
AND target.ENTITY_NAME = source.ENTITY_NAME
WHEN MATCHED THEN UPDATE SET
    RULE_DESCRIPTION = source.RULE_DESCRIPTION,
    SEVERITY = source.SEVERITY,
    IS_ACTIVE = source.IS_ACTIVE
WHEN NOT MATCHED THEN INSERT (
    RULE_CODE,
    ENTITY_NAME,
    RULE_DESCRIPTION,
    SEVERITY,
    IS_ACTIVE
) VALUES (
    source.RULE_CODE,
    source.ENTITY_NAME,
    source.RULE_DESCRIPTION,
    source.SEVERITY,
    source.IS_ACTIVE
);

SHOW TABLES IN SCHEMA ABC_RETAIL.GOVERNANCE;

SELECT *
FROM ABC_RETAIL.GOVERNANCE.DQ_RULES
ORDER BY RULE_CODE, ENTITY_NAME;
