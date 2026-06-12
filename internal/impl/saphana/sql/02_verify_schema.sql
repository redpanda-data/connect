-- =============================================================================
-- 02_verify_schema.sql — CDC Verification Schema
-- =============================================================================
-- Creates the _CDC_VERIFY schema and the CHANGES audit table that trigger-based
-- CDC verification writes into.  The triggers in 03_triggers.sql write one row
-- here per DML operation so test assertions can query a single table instead of
-- joining against the source tables.
--
-- This schema acts as the "expected" side when comparing against a real CDC
-- log-reader's output.
-- =============================================================================

-- ---------------------------------------------------------------------------
-- Schema
-- ---------------------------------------------------------------------------
DO BEGIN
    DECLARE schema_exists CONDITION FOR SQL_ERROR_CODE 386;
    DECLARE EXIT HANDLER FOR schema_exists BEGIN END;
    EXEC 'CREATE SCHEMA _CDC_VERIFY';
END;

-- ---------------------------------------------------------------------------
-- CHANGES — unified audit table written by all CDC_TEST triggers
-- ---------------------------------------------------------------------------
DROP TABLE _CDC_VERIFY.CHANGES CASCADE;
CREATE COLUMN TABLE _CDC_VERIFY.CHANGES (
    -- Auto-incrementing surrogate key; preserves strict insertion order.
    ID          BIGINT          GENERATED ALWAYS AS IDENTITY PRIMARY KEY,

    -- Operation code: 'I' = INSERT, 'U' = UPDATE, 'D' = DELETE
    OP          VARCHAR(1)      NOT NULL,

    -- Source table coordinates
    SCHEMA_NAME VARCHAR(256)    NOT NULL,
    TABLE_NAME  VARCHAR(256)    NOT NULL,

    -- Wall-clock time at trigger fire (approximate; use ID for ordering)
    OP_TIME     TIMESTAMP       DEFAULT CURRENT_TIMESTAMP,

    -- JSON: primary key column(s) and their values at the time of the operation.
    -- For NO_PK table: a synthetic identifier using STR_VAL + INT_VAL.
    PK_JSON     NCLOB,

    -- JSON: before image — populated for UPDATE (old values) and DELETE.
    -- NULL for INSERT rows.
    OLD_VALUES  NCLOB,

    -- JSON: after image — populated for INSERT and UPDATE (new values).
    -- NULL for DELETE rows.
    NEW_VALUES  NCLOB
);

-- ---------------------------------------------------------------------------
-- Verify structure
-- ---------------------------------------------------------------------------
SELECT COLUMN_NAME, DATA_TYPE_NAME, IS_NULLABLE, DEFAULT_VALUE
FROM SYS.TABLE_COLUMNS
WHERE SCHEMA_NAME = '_CDC_VERIFY' AND TABLE_NAME = 'CHANGES'
ORDER BY POSITION;
