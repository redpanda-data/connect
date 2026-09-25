-- Oracle Database Benchmark - Delete Users Data
SET SERVEROUTPUT ON;

-- Switch to testdb schema
ALTER SESSION SET CURRENT_SCHEMA = testdb;
/

DECLARE
    delete_total  NUMBER := NVL(TO_NUMBER('&1', '9999999999'), 1000);
    delete_batch  NUMBER := 10000;
    deleted_so_far NUMBER := 0;
    rows_deleted  NUMBER;
BEGIN
    DBMS_OUTPUT.PUT_LINE('Deleting ' || delete_total || ' rows from testdb.users...');

    WHILE deleted_so_far < delete_total
    LOOP
        DELETE FROM testdb.users
        WHERE id IN (
            SELECT id FROM testdb.users
            WHERE ROWNUM <= LEAST(delete_batch, delete_total - deleted_so_far)
        );

        rows_deleted   := SQL%ROWCOUNT;
        deleted_so_far := deleted_so_far + rows_deleted;
        COMMIT;

        DBMS_OUTPUT.PUT_LINE('Progress: ' || deleted_so_far || '/' || delete_total || ' rows deleted from testdb.users');

        EXIT WHEN rows_deleted = 0;
    END LOOP;

    DBMS_OUTPUT.PUT_LINE('Completed: ' || deleted_so_far || ' rows deleted from testdb.users');
END;
/

-- Verification
DECLARE
    users_count NUMBER;
BEGIN
    SELECT COUNT(*) INTO users_count FROM testdb.users;
    DBMS_OUTPUT.PUT_LINE('Verification - testdb.users: ' || users_count || ' rows remaining');
END;
/
