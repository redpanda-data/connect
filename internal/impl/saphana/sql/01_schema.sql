-- =============================================================================
-- 01_schema.sql — CDC Test Schema and Table Definitions
-- =============================================================================
-- Creates all test tables for SAP HANA CDC redo-log ground-truth coverage.
-- Run as SYSTEM (or a user with CREATE SCHEMA / CREATE TABLE privileges).
--
-- CDC redo-log block types exercised by this test suite:
--   insert           → INSERT statements
--   update           → UPDATE statements (before+delta in log)
--   delete           → DELETE statements (rowid only in log)
--   upsert           → UPSERT / REPLACE (HANA native; NOT decomposed into insert+update)
--   truncate         → TRUNCATE TABLE (NOT captured by per-row triggers — by design)
--   commit           → transaction COMMIT
--   rollback         → transaction ROLLBACK (all buffered changes discarded)
--   savepoint        → internal HANA persistence savepoints
--   dictionary change→ DDL (CREATE / ALTER / DROP TABLE)
-- =============================================================================

-- ---------------------------------------------------------------------------
-- Schema
-- ---------------------------------------------------------------------------
-- HANA does not support CREATE SCHEMA IF NOT EXISTS, use exception handler.
DO BEGIN
    DECLARE schema_exists CONDITION FOR SQL_ERROR_CODE 386;
    DECLARE EXIT HANDLER FOR schema_exists BEGIN END;
    EXEC 'CREATE SCHEMA CDC_TEST';
END;

-- ---------------------------------------------------------------------------
-- 1. BASIC — primary workhorse for all DML tests
--    Integer PK + common business types.  Triggers are created in 03_triggers.sql.
-- ---------------------------------------------------------------------------
DROP TABLE CDC_TEST.BASIC CASCADE;
CREATE COLUMN TABLE CDC_TEST.BASIC (
    ID          INTEGER     NOT NULL PRIMARY KEY,
    STR_VAL     VARCHAR(255),
    NSTR_VAL    NVARCHAR(255),
    INT_VAL     INTEGER,
    BIGINT_VAL  BIGINT,
    DOUBLE_VAL  DOUBLE,
    DEC_VAL     DECIMAL(15,4),
    DATE_VAL    DATE,
    TS_VAL      TIMESTAMP,
    BOOL_VAL    BOOLEAN
);

-- ---------------------------------------------------------------------------
-- 2. ALL_TYPES — one column per HANA data type
--    Used for type-mapping ground-truth.  No triggers (tested via direct query).
-- ---------------------------------------------------------------------------
DROP TABLE CDC_TEST.ALL_TYPES CASCADE;
CREATE COLUMN TABLE CDC_TEST.ALL_TYPES (
    ID              INTEGER     NOT NULL PRIMARY KEY,
    -- Exact integer types
    TINYINT_VAL     TINYINT,
    SMALLINT_VAL    SMALLINT,
    INTEGER_VAL     INTEGER,
    BIGINT_VAL      BIGINT,
    -- Floating point
    REAL_VAL        REAL,
    DOUBLE_VAL      DOUBLE,
    -- Fixed-precision decimals
    DECIMAL_10_2    DECIMAL(10,2),
    DECIMAL_38_10   DECIMAL(38,10),
    SMALLDECIMAL_VAL SMALLDECIMAL,
    -- Character strings
    VARCHAR_VAL     VARCHAR(500),
    NVARCHAR_VAL    NVARCHAR(500),
    CHAR_VAL        CHAR(10),
    NCHAR_VAL       NCHAR(10),
    ALPHANUM_VAL    ALPHANUM(10),
    SHORTTEXT_VAL   SHORTTEXT(100),
    -- Binary
    BINARY_VAL      BINARY(16),
    VARBINARY_VAL   VARBINARY(256),
    -- Date/time
    DATE_VAL        DATE,
    TIME_VAL        TIME,
    SECONDDATE_VAL  SECONDDATE,
    TIMESTAMP_VAL   TIMESTAMP,
    LONGDATE_VAL    LONGDATE,
    -- Boolean
    BOOLEAN_VAL     BOOLEAN
);

-- ---------------------------------------------------------------------------
-- 3. LOB_TABLE — CLOB, NCLOB, BLOB alongside a VARCHAR key
--    Triggers use LENGTH()/OCTET_LENGTH() rather than embedding LOB content.
-- ---------------------------------------------------------------------------
DROP TABLE CDC_TEST.LOB_TABLE CASCADE;
CREATE COLUMN TABLE CDC_TEST.LOB_TABLE (
    ID          INTEGER     NOT NULL PRIMARY KEY,
    STR_VAL     VARCHAR(255),
    CLOB_VAL    CLOB,
    NCLOB_VAL   NCLOB,
    BLOB_VAL    BLOB
);

-- ---------------------------------------------------------------------------
-- 4. NO_PK — table WITHOUT a primary key
--    Edge case: CDC readers must fall back to rowid-based change identification.
--    Only an INSERT trigger is created (no stable identity to correlate UPDATE/DELETE).
-- ---------------------------------------------------------------------------
DROP TABLE CDC_TEST.NO_PK CASCADE;
CREATE COLUMN TABLE CDC_TEST.NO_PK (
    STR_VAL     VARCHAR(255),
    INT_VAL     INTEGER,
    TS_VAL      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ---------------------------------------------------------------------------
-- 5. COMPOSITE_PK — multi-column primary key
--    Tests PK serialization in CDC events (both columns must appear in PK payload).
-- ---------------------------------------------------------------------------
DROP TABLE CDC_TEST.COMPOSITE_PK CASCADE;
CREATE COLUMN TABLE CDC_TEST.COMPOSITE_PK (
    PK1         INTEGER     NOT NULL,
    PK2         VARCHAR(50) NOT NULL,
    STR_VAL     VARCHAR(255),
    INT_VAL     INTEGER,
    TS_VAL      TIMESTAMP,
    PRIMARY KEY (PK1, PK2)
);

-- ---------------------------------------------------------------------------
-- 6. DDL_TARGET — lightweight table for DDL (ALTER/DROP/RENAME) tests
--    This table is recreated per test run; its existence here is only for
--    initial setup.  Tests that need dictionary-change events will DROP and
--    re-CREATE or ALTER this table.
-- ---------------------------------------------------------------------------
DROP TABLE CDC_TEST.DDL_TARGET CASCADE;
CREATE COLUMN TABLE CDC_TEST.DDL_TARGET (
    ID      INTEGER     NOT NULL PRIMARY KEY,
    VAL     VARCHAR(100)
);

-- ---------------------------------------------------------------------------
-- 7. ROW_TABLE — explicitly a ROW STORE table (not column store)
--    CDC tools (HVR, Debezium HANA connector, rtdi.io) explicitly do NOT
--    support row-store tables.  Included to document and assert that difference.
--    No triggers are created on this table.
-- ---------------------------------------------------------------------------
DROP TABLE CDC_TEST.ROW_TABLE CASCADE;
CREATE ROW TABLE CDC_TEST.ROW_TABLE (
    ID      INTEGER     NOT NULL PRIMARY KEY,
    VAL     VARCHAR(255),
    TS_VAL  TIMESTAMP
);

-- ---------------------------------------------------------------------------
-- 8. UPSERT_TEST — dedicated table for UPSERT/REPLACE coverage
--    HANA's UPSERT generates a distinct "upsert" block in the redo log —
--    it is NOT decomposed into insert+update.  Unique key constraint tests
--    conflict behavior.
-- ---------------------------------------------------------------------------
DROP TABLE CDC_TEST.UPSERT_TEST CASCADE;
CREATE COLUMN TABLE CDC_TEST.UPSERT_TEST (
    ID          INTEGER     NOT NULL PRIMARY KEY,
    STR_VAL     VARCHAR(255),
    INT_VAL     INTEGER
);

-- ---------------------------------------------------------------------------
-- Verify table count
-- ---------------------------------------------------------------------------
SELECT
    TABLE_NAME,
    TABLE_TYPE,
    STORE_TYPE
FROM SYS.TABLES
WHERE SCHEMA_NAME = 'CDC_TEST'
ORDER BY TABLE_NAME;
