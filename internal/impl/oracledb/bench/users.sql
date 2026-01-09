-- Oracle Database Benchmark - Users Data
-- Connection: oracle://system:oracle@localhost:1521/XEPDB1
-- Prerequisites: Run create.sql first

-- Enable output for debugging
SET SERVEROUTPUT ON;

-- Switch to testdb schema
ALTER SESSION SET CURRENT_SCHEMA = testdb;
/

DECLARE
    users_total NUMBER := 150000;
    users_batch_size NUMBER := 10000;
    users_current NUMBER := 0;
    users_batch_end NUMBER;
    about_text CLOB;
BEGIN
    DBMS_OUTPUT.PUT_LINE('Inserting test data into testdb.users (' || users_total || ' rows)...');

    WHILE users_current < users_total
    LOOP
        users_batch_end := users_current + users_batch_size;
        IF users_batch_end > users_total THEN
            users_batch_end := users_total;
        END IF;

        -- Insert batch using a CTE-style approach
        INSERT INTO testdb.users (name, surname, about, email, date_of_birth, join_date, created_at, is_active, login_count, balance)
        SELECT
            'user-' || n,                                                    -- name
            'surname-' || n,                                                 -- surname
            RPAD('This is about user ' || n || '. ', 500000, 'X'),          -- about ~500 KB
            'user' || n || '@example.com',                                   -- email
            SYSDATE - MOD(n, 10000),                                         -- date_of_birth, spread over ~27 years
            SYSTIMESTAMP,                                                    -- join_date
            SYSTIMESTAMP,                                                    -- created_at
            CASE WHEN MOD(n, 2) = 0 THEN 1 ELSE 0 END,                      -- is_active alternating 1/0
            MOD(n, 100),                                                     -- login_count between 0-99
            CAST(MOD(n, 1000) + MOD(n, 100) / 100.0 AS NUMBER(10,2))        -- balance
        FROM (
            SELECT ROWNUM + users_current AS n
            FROM dual
            CONNECT BY LEVEL <= (users_batch_end - users_current)
        );

        COMMIT;

        users_current := users_batch_end;

        -- Log progress after every batch
        DBMS_OUTPUT.PUT_LINE('Progress: ' || users_current || '/' || users_total || ' rows inserted into testdb.users');
    END LOOP;

    DBMS_OUTPUT.PUT_LINE('Completed: ' || users_current || ' rows inserted into testdb.users');
END;
/

-- Verification
DECLARE
    users_count NUMBER;
BEGIN
    SELECT COUNT(*) INTO users_count FROM testdb.users;
    DBMS_OUTPUT.PUT_LINE('Verification - testdb.users: ' || users_count || ' rows');
END;
/
