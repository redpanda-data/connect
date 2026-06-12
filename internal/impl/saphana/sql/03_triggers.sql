-- =============================================================================
-- 03_triggers.sql — CDC Verification Triggers
-- =============================================================================
-- AFTER INSERT / UPDATE / DELETE triggers on CDC_TEST tables.
-- Each trigger writes one row to _CDC_VERIFY.CHANGES.
--
-- Tables covered:
--   CDC_TEST.BASIC          — all 3 triggers (full column capture)
--   CDC_TEST.LOB_TABLE      — all 3 triggers (LOB length, not LOB content)
--   CDC_TEST.COMPOSITE_PK   — all 3 triggers (both PK columns in PK_JSON)
--   CDC_TEST.UPSERT_TEST    — all 3 triggers
--   CDC_TEST.NO_PK          — INSERT trigger only (no stable PK)
--
-- Tables intentionally excluded:
--   CDC_TEST.ALL_TYPES      — type coverage tested via direct query, not triggers
--   CDC_TEST.ROW_TABLE      — row-store; CDC tools do not support row-store tables
--   CDC_TEST.DDL_TARGET     — DDL test table; no DML trigger needed
--
-- HANA trigger syntax notes:
--   • CREATE OR REPLACE TRIGGER <name>
--   • :new.<col> and :old.<col>  (colon prefix — SQLScript row variable syntax)
--   • JSON_OBJECT('KEY' VALUE :new.COL, ...) — SQL/JSON standard (SPS 03+)
--   • JSON_OBJECT naturally serialises NULL values as JSON null
--   • LOB columns cannot be embedded in JSON_OBJECT; use LENGTH() / OCTET_LENGTH()
-- =============================================================================


-- =============================================================================
-- CDC_TEST.BASIC triggers
-- =============================================================================

-- ---------------------------------------------------------------------------
-- BASIC: INSERT
-- ---------------------------------------------------------------------------
CREATE OR REPLACE TRIGGER cdc_verify_basic_ins
AFTER INSERT ON CDC_TEST.BASIC
FOR EACH ROW
BEGIN
    INSERT INTO _CDC_VERIFY.CHANGES (OP, SCHEMA_NAME, TABLE_NAME, PK_JSON, NEW_VALUES)
    VALUES (
        'I',
        'CDC_TEST',
        'BASIC',
        JSON_OBJECT(
            'ID' VALUE :new.ID
        ),
        JSON_OBJECT(
            'ID'         VALUE :new.ID,
            'STR_VAL'    VALUE :new.STR_VAL,
            'NSTR_VAL'   VALUE :new.NSTR_VAL,
            'INT_VAL'    VALUE :new.INT_VAL,
            'BIGINT_VAL' VALUE :new.BIGINT_VAL,
            'DOUBLE_VAL' VALUE :new.DOUBLE_VAL,
            'DEC_VAL'    VALUE :new.DEC_VAL,
            'DATE_VAL'   VALUE :new.DATE_VAL,
            'TS_VAL'     VALUE :new.TS_VAL,
            'BOOL_VAL'   VALUE :new.BOOL_VAL
        )
    );
END;

-- ---------------------------------------------------------------------------
-- BASIC: UPDATE
-- ---------------------------------------------------------------------------
CREATE OR REPLACE TRIGGER cdc_verify_basic_upd
AFTER UPDATE ON CDC_TEST.BASIC
FOR EACH ROW
BEGIN
    INSERT INTO _CDC_VERIFY.CHANGES (OP, SCHEMA_NAME, TABLE_NAME, PK_JSON, OLD_VALUES, NEW_VALUES)
    VALUES (
        'U',
        'CDC_TEST',
        'BASIC',
        JSON_OBJECT(
            'ID' VALUE :new.ID
        ),
        JSON_OBJECT(
            'ID'         VALUE :old.ID,
            'STR_VAL'    VALUE :old.STR_VAL,
            'NSTR_VAL'   VALUE :old.NSTR_VAL,
            'INT_VAL'    VALUE :old.INT_VAL,
            'BIGINT_VAL' VALUE :old.BIGINT_VAL,
            'DOUBLE_VAL' VALUE :old.DOUBLE_VAL,
            'DEC_VAL'    VALUE :old.DEC_VAL,
            'DATE_VAL'   VALUE :old.DATE_VAL,
            'TS_VAL'     VALUE :old.TS_VAL,
            'BOOL_VAL'   VALUE :old.BOOL_VAL
        ),
        JSON_OBJECT(
            'ID'         VALUE :new.ID,
            'STR_VAL'    VALUE :new.STR_VAL,
            'NSTR_VAL'   VALUE :new.NSTR_VAL,
            'INT_VAL'    VALUE :new.INT_VAL,
            'BIGINT_VAL' VALUE :new.BIGINT_VAL,
            'DOUBLE_VAL' VALUE :new.DOUBLE_VAL,
            'DEC_VAL'    VALUE :new.DEC_VAL,
            'DATE_VAL'   VALUE :new.DATE_VAL,
            'TS_VAL'     VALUE :new.TS_VAL,
            'BOOL_VAL'   VALUE :new.BOOL_VAL
        )
    );
END;

-- ---------------------------------------------------------------------------
-- BASIC: DELETE
-- ---------------------------------------------------------------------------
CREATE OR REPLACE TRIGGER cdc_verify_basic_del
AFTER DELETE ON CDC_TEST.BASIC
FOR EACH ROW
BEGIN
    INSERT INTO _CDC_VERIFY.CHANGES (OP, SCHEMA_NAME, TABLE_NAME, PK_JSON, OLD_VALUES)
    VALUES (
        'D',
        'CDC_TEST',
        'BASIC',
        JSON_OBJECT(
            'ID' VALUE :old.ID
        ),
        JSON_OBJECT(
            'ID'         VALUE :old.ID,
            'STR_VAL'    VALUE :old.STR_VAL,
            'NSTR_VAL'   VALUE :old.NSTR_VAL,
            'INT_VAL'    VALUE :old.INT_VAL,
            'BIGINT_VAL' VALUE :old.BIGINT_VAL,
            'DOUBLE_VAL' VALUE :old.DOUBLE_VAL,
            'DEC_VAL'    VALUE :old.DEC_VAL,
            'DATE_VAL'   VALUE :old.DATE_VAL,
            'TS_VAL'     VALUE :old.TS_VAL,
            'BOOL_VAL'   VALUE :old.BOOL_VAL
        )
    );
END;


-- =============================================================================
-- CDC_TEST.LOB_TABLE triggers
-- =============================================================================
-- LOB columns (CLOB, NCLOB, BLOB) cannot be directly embedded in JSON_OBJECT.
-- Instead we record:
--   -1  → column is NULL
--   >= 0 → byte/character length of the LOB value
-- This is testable without materialising LOB content in the verify table.

-- ---------------------------------------------------------------------------
-- LOB_TABLE: INSERT
-- ---------------------------------------------------------------------------
CREATE OR REPLACE TRIGGER cdc_verify_lob_ins
AFTER INSERT ON CDC_TEST.LOB_TABLE
FOR EACH ROW
BEGIN
    INSERT INTO _CDC_VERIFY.CHANGES (OP, SCHEMA_NAME, TABLE_NAME, PK_JSON, NEW_VALUES)
    VALUES (
        'I',
        'CDC_TEST',
        'LOB_TABLE',
        JSON_OBJECT(
            'ID' VALUE :new.ID
        ),
        JSON_OBJECT(
            'ID'         VALUE :new.ID,
            'STR_VAL'    VALUE :new.STR_VAL,
            'CLOB_LEN'   VALUE CASE WHEN :new.CLOB_VAL IS NULL THEN -1 ELSE LENGTH(:new.CLOB_VAL) END,
            'NCLOB_LEN'  VALUE CASE WHEN :new.NCLOB_VAL IS NULL THEN -1 ELSE LENGTH(:new.NCLOB_VAL) END,
            'BLOB_LEN'   VALUE CASE WHEN :new.BLOB_VAL IS NULL THEN -1 ELSE OCTET_LENGTH(:new.BLOB_VAL) END
        )
    );
END;

-- ---------------------------------------------------------------------------
-- LOB_TABLE: UPDATE
-- ---------------------------------------------------------------------------
CREATE OR REPLACE TRIGGER cdc_verify_lob_upd
AFTER UPDATE ON CDC_TEST.LOB_TABLE
FOR EACH ROW
BEGIN
    INSERT INTO _CDC_VERIFY.CHANGES (OP, SCHEMA_NAME, TABLE_NAME, PK_JSON, OLD_VALUES, NEW_VALUES)
    VALUES (
        'U',
        'CDC_TEST',
        'LOB_TABLE',
        JSON_OBJECT(
            'ID' VALUE :new.ID
        ),
        JSON_OBJECT(
            'ID'         VALUE :old.ID,
            'STR_VAL'    VALUE :old.STR_VAL,
            'CLOB_LEN'   VALUE CASE WHEN :old.CLOB_VAL IS NULL THEN -1 ELSE LENGTH(:old.CLOB_VAL) END,
            'NCLOB_LEN'  VALUE CASE WHEN :old.NCLOB_VAL IS NULL THEN -1 ELSE LENGTH(:old.NCLOB_VAL) END,
            'BLOB_LEN'   VALUE CASE WHEN :old.BLOB_VAL IS NULL THEN -1 ELSE OCTET_LENGTH(:old.BLOB_VAL) END
        ),
        JSON_OBJECT(
            'ID'         VALUE :new.ID,
            'STR_VAL'    VALUE :new.STR_VAL,
            'CLOB_LEN'   VALUE CASE WHEN :new.CLOB_VAL IS NULL THEN -1 ELSE LENGTH(:new.CLOB_VAL) END,
            'NCLOB_LEN'  VALUE CASE WHEN :new.NCLOB_VAL IS NULL THEN -1 ELSE LENGTH(:new.NCLOB_VAL) END,
            'BLOB_LEN'   VALUE CASE WHEN :new.BLOB_VAL IS NULL THEN -1 ELSE OCTET_LENGTH(:new.BLOB_VAL) END
        )
    );
END;

-- ---------------------------------------------------------------------------
-- LOB_TABLE: DELETE
-- ---------------------------------------------------------------------------
CREATE OR REPLACE TRIGGER cdc_verify_lob_del
AFTER DELETE ON CDC_TEST.LOB_TABLE
FOR EACH ROW
BEGIN
    INSERT INTO _CDC_VERIFY.CHANGES (OP, SCHEMA_NAME, TABLE_NAME, PK_JSON, OLD_VALUES)
    VALUES (
        'D',
        'CDC_TEST',
        'LOB_TABLE',
        JSON_OBJECT(
            'ID' VALUE :old.ID
        ),
        JSON_OBJECT(
            'ID'         VALUE :old.ID,
            'STR_VAL'    VALUE :old.STR_VAL,
            'CLOB_LEN'   VALUE CASE WHEN :old.CLOB_VAL IS NULL THEN -1 ELSE LENGTH(:old.CLOB_VAL) END,
            'NCLOB_LEN'  VALUE CASE WHEN :old.NCLOB_VAL IS NULL THEN -1 ELSE LENGTH(:old.NCLOB_VAL) END,
            'BLOB_LEN'   VALUE CASE WHEN :old.BLOB_VAL IS NULL THEN -1 ELSE OCTET_LENGTH(:old.BLOB_VAL) END
        )
    );
END;


-- =============================================================================
-- CDC_TEST.COMPOSITE_PK triggers
-- =============================================================================
-- PK_JSON must contain BOTH PK columns so test assertions can correlate
-- events across a multi-column primary key.

-- ---------------------------------------------------------------------------
-- COMPOSITE_PK: INSERT
-- ---------------------------------------------------------------------------
CREATE OR REPLACE TRIGGER cdc_verify_cpk_ins
AFTER INSERT ON CDC_TEST.COMPOSITE_PK
FOR EACH ROW
BEGIN
    INSERT INTO _CDC_VERIFY.CHANGES (OP, SCHEMA_NAME, TABLE_NAME, PK_JSON, NEW_VALUES)
    VALUES (
        'I',
        'CDC_TEST',
        'COMPOSITE_PK',
        JSON_OBJECT(
            'PK1' VALUE :new.PK1,
            'PK2' VALUE :new.PK2
        ),
        JSON_OBJECT(
            'PK1'     VALUE :new.PK1,
            'PK2'     VALUE :new.PK2,
            'STR_VAL' VALUE :new.STR_VAL,
            'INT_VAL' VALUE :new.INT_VAL,
            'TS_VAL'  VALUE :new.TS_VAL
        )
    );
END;

-- ---------------------------------------------------------------------------
-- COMPOSITE_PK: UPDATE
-- ---------------------------------------------------------------------------
CREATE OR REPLACE TRIGGER cdc_verify_cpk_upd
AFTER UPDATE ON CDC_TEST.COMPOSITE_PK
FOR EACH ROW
BEGIN
    INSERT INTO _CDC_VERIFY.CHANGES (OP, SCHEMA_NAME, TABLE_NAME, PK_JSON, OLD_VALUES, NEW_VALUES)
    VALUES (
        'U',
        'CDC_TEST',
        'COMPOSITE_PK',
        JSON_OBJECT(
            'PK1' VALUE :new.PK1,
            'PK2' VALUE :new.PK2
        ),
        JSON_OBJECT(
            'PK1'     VALUE :old.PK1,
            'PK2'     VALUE :old.PK2,
            'STR_VAL' VALUE :old.STR_VAL,
            'INT_VAL' VALUE :old.INT_VAL,
            'TS_VAL'  VALUE :old.TS_VAL
        ),
        JSON_OBJECT(
            'PK1'     VALUE :new.PK1,
            'PK2'     VALUE :new.PK2,
            'STR_VAL' VALUE :new.STR_VAL,
            'INT_VAL' VALUE :new.INT_VAL,
            'TS_VAL'  VALUE :new.TS_VAL
        )
    );
END;

-- ---------------------------------------------------------------------------
-- COMPOSITE_PK: DELETE
-- ---------------------------------------------------------------------------
CREATE OR REPLACE TRIGGER cdc_verify_cpk_del
AFTER DELETE ON CDC_TEST.COMPOSITE_PK
FOR EACH ROW
BEGIN
    INSERT INTO _CDC_VERIFY.CHANGES (OP, SCHEMA_NAME, TABLE_NAME, PK_JSON, OLD_VALUES)
    VALUES (
        'D',
        'CDC_TEST',
        'COMPOSITE_PK',
        JSON_OBJECT(
            'PK1' VALUE :old.PK1,
            'PK2' VALUE :old.PK2
        ),
        JSON_OBJECT(
            'PK1'     VALUE :old.PK1,
            'PK2'     VALUE :old.PK2,
            'STR_VAL' VALUE :old.STR_VAL,
            'INT_VAL' VALUE :old.INT_VAL,
            'TS_VAL'  VALUE :old.TS_VAL
        )
    );
END;


-- =============================================================================
-- CDC_TEST.UPSERT_TEST triggers
-- =============================================================================
-- HANA's UPSERT statement produces a single "upsert" block in the redo log
-- (not a separate insert + update).  These triggers capture which DML path
-- HANA actually executed (INSERT branch vs UPDATE branch) so tests can assert
-- what the CDC reader sees vs what the application intended.

-- ---------------------------------------------------------------------------
-- UPSERT_TEST: INSERT
-- ---------------------------------------------------------------------------
CREATE OR REPLACE TRIGGER cdc_verify_upsert_ins
AFTER INSERT ON CDC_TEST.UPSERT_TEST
FOR EACH ROW
BEGIN
    INSERT INTO _CDC_VERIFY.CHANGES (OP, SCHEMA_NAME, TABLE_NAME, PK_JSON, NEW_VALUES)
    VALUES (
        'I',
        'CDC_TEST',
        'UPSERT_TEST',
        JSON_OBJECT(
            'ID' VALUE :new.ID
        ),
        JSON_OBJECT(
            'ID'      VALUE :new.ID,
            'STR_VAL' VALUE :new.STR_VAL,
            'INT_VAL' VALUE :new.INT_VAL
        )
    );
END;

-- ---------------------------------------------------------------------------
-- UPSERT_TEST: UPDATE
-- ---------------------------------------------------------------------------
CREATE OR REPLACE TRIGGER cdc_verify_upsert_upd
AFTER UPDATE ON CDC_TEST.UPSERT_TEST
FOR EACH ROW
BEGIN
    INSERT INTO _CDC_VERIFY.CHANGES (OP, SCHEMA_NAME, TABLE_NAME, PK_JSON, OLD_VALUES, NEW_VALUES)
    VALUES (
        'U',
        'CDC_TEST',
        'UPSERT_TEST',
        JSON_OBJECT(
            'ID' VALUE :new.ID
        ),
        JSON_OBJECT(
            'ID'      VALUE :old.ID,
            'STR_VAL' VALUE :old.STR_VAL,
            'INT_VAL' VALUE :old.INT_VAL
        ),
        JSON_OBJECT(
            'ID'      VALUE :new.ID,
            'STR_VAL' VALUE :new.STR_VAL,
            'INT_VAL' VALUE :new.INT_VAL
        )
    );
END;

-- ---------------------------------------------------------------------------
-- UPSERT_TEST: DELETE
-- ---------------------------------------------------------------------------
CREATE OR REPLACE TRIGGER cdc_verify_upsert_del
AFTER DELETE ON CDC_TEST.UPSERT_TEST
FOR EACH ROW
BEGIN
    INSERT INTO _CDC_VERIFY.CHANGES (OP, SCHEMA_NAME, TABLE_NAME, PK_JSON, OLD_VALUES)
    VALUES (
        'D',
        'CDC_TEST',
        'UPSERT_TEST',
        JSON_OBJECT(
            'ID' VALUE :old.ID
        ),
        JSON_OBJECT(
            'ID'      VALUE :old.ID,
            'STR_VAL' VALUE :old.STR_VAL,
            'INT_VAL' VALUE :old.INT_VAL
        )
    );
END;


-- =============================================================================
-- CDC_TEST.NO_PK triggers
-- =============================================================================
-- This table has no primary key.  CDC readers must use HANA's internal rowid
-- for change correlation.  We create only an INSERT trigger because without a
-- stable PK we cannot reliably link UPDATE/DELETE events to a specific earlier
-- row in the verify table.
--
-- For PK_JSON we record the identifying-looking columns (STR_VAL + INT_VAL) as
-- a synthetic composite identifier for test correlation purposes.

-- ---------------------------------------------------------------------------
-- NO_PK: INSERT only
-- ---------------------------------------------------------------------------
CREATE OR REPLACE TRIGGER cdc_verify_nopk_ins
AFTER INSERT ON CDC_TEST.NO_PK
FOR EACH ROW
BEGIN
    INSERT INTO _CDC_VERIFY.CHANGES (OP, SCHEMA_NAME, TABLE_NAME, PK_JSON, NEW_VALUES)
    VALUES (
        'I',
        'CDC_TEST',
        'NO_PK',
        JSON_OBJECT(
            'STR_VAL' VALUE :new.STR_VAL,
            'INT_VAL' VALUE :new.INT_VAL
        ),
        JSON_OBJECT(
            'STR_VAL' VALUE :new.STR_VAL,
            'INT_VAL' VALUE :new.INT_VAL,
            'TS_VAL'  VALUE :new.TS_VAL
        )
    );
END;


-- =============================================================================
-- Verify trigger registration
-- =============================================================================
SELECT
    TRIGGER_NAME,
    SUBJECT_TABLE_SCHEMA,
    SUBJECT_TABLE_NAME,
    TRIGGER_ACTION_TIME,
    TRIGGER_EVENT
FROM SYS.TRIGGERS
WHERE TRIGGER_NAME LIKE 'CDC_VERIFY_%'
ORDER BY SUBJECT_TABLE_NAME, TRIGGER_EVENT;
