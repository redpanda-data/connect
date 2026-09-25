-- Creates, opens, and persists the TESTPDB pluggable database.
-- Must be run as SYSDBA from within the Oracle container.
DECLARE
    pdb_exists NUMBER;
BEGIN
    SELECT COUNT(*) INTO pdb_exists FROM v$pdbs WHERE name = 'TESTPDB';

    IF pdb_exists = 0 THEN
        EXECUTE IMMEDIATE q'[
            CREATE PLUGGABLE DATABASE testpdb
            ADMIN USER pdbadmin IDENTIFIED BY pdbadmin123
            ROLES = (DBA)
            FILE_NAME_CONVERT = (
                '/opt/oracle/oradata/FREE/pdbseed/',
                '/opt/oracle/oradata/FREE/testpdb/'
            )
        ]';
        DBMS_OUTPUT.PUT_LINE('PDB TESTPDB created');
    ELSE
        DBMS_OUTPUT.PUT_LINE('PDB TESTPDB already exists');
    END IF;
END;
/

ALTER PLUGGABLE DATABASE testpdb OPEN;
ALTER PLUGGABLE DATABASE testpdb SAVE STATE;

SELECT name, open_mode FROM v$pdbs WHERE name = 'TESTPDB';

-- Force dynamic service registration so TESTPDB is immediately reachable via the listener
ALTER SYSTEM REGISTER;

EXIT;
