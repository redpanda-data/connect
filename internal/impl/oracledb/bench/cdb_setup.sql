-- ============================================================================
-- STAGE 1: Create C##TESTDB in CDB$ROOT
-- ============================================================================

CREATE USER IF NOT EXISTS c##testdb IDENTIFIED BY testdb123 CONTAINER=ALL;
/

GRANT CONNECT TO c##testdb CONTAINER=ALL;
/
GRANT DBA TO c##testdb CONTAINER=ALL;
/
GRANT LOGMINING TO c##testdb CONTAINER=ALL;
/
GRANT SELECT ANY DICTIONARY TO c##testdb CONTAINER=ALL;
/
GRANT UNLIMITED TABLESPACE TO c##testdb CONTAINER=ALL;
/
GRANT EXECUTE ON SYS.DBMS_LOGMNR TO c##testdb CONTAINER=ALL;
/
GRANT EXECUTE ON SYS.DBMS_LOGMNR_D TO c##testdb CONTAINER=ALL;
/
ALTER USER c##testdb SET CONTAINER_DATA = ALL CONTAINER = CURRENT;
/
GRANT SET CONTAINER TO c##testdb CONTAINER=ALL;
/

-- ============================================================================
-- STAGE 2: Create C##RPCN in CDB$ROOT
-- ============================================================================

CREATE USER IF NOT EXISTS c##rpcn IDENTIFIED BY rpcn123 CONTAINER=ALL;
/

GRANT CONNECT TO c##rpcn CONTAINER=ALL;
/
GRANT RESOURCE TO c##rpcn CONTAINER=ALL;
/
GRANT UNLIMITED TABLESPACE TO c##rpcn CONTAINER=ALL;
/

-- ============================================================================
-- STAGE 3: Unlock C##TESTDB in TESTPDB
-- Container switching must happen at SQL*Plus level, not inside PL/SQL blocks.
-- ============================================================================

ALTER SESSION SET CONTAINER = TESTPDB;
/

ALTER USER c##testdb ACCOUNT UNLOCK;
/

ALTER SESSION SET CONTAINER = CDB$ROOT;
/

BEGIN
    DBMS_OUTPUT.PUT_LINE('');
    DBMS_OUTPUT.PUT_LINE('=== CDB user setup complete ===');
    DBMS_OUTPUT.PUT_LINE('connection_string : oracle://c%23%23testdb:testdb123@localhost:1521/FREE');
    DBMS_OUTPUT.PUT_LINE('pdb_name          : TESTPDB');
    DBMS_OUTPUT.PUT_LINE('Checkpoint cache  : C##RPCN.CDC_CHECKPOINT_TESTPDB (auto-created)');
END;
/
