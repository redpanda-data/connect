-- Oracle Database Benchmark - Cart Data
-- Connection: oracle://system:oracle@localhost:1521/XEPDB1
-- Prerequisites: Run create.sql first

-- Enable output for debugging
SET SERVEROUTPUT ON;

-- Switch to testdb schema
ALTER SESSION SET CURRENT_SCHEMA = testdb;
/

DECLARE
    cart_total NUMBER := 10000000;
    cart_batch_size NUMBER := 10000;
    cart_current NUMBER := 0;
    batch_end NUMBER;
BEGIN
    DBMS_OUTPUT.PUT_LINE('Inserting test data into testdb.cart (' || cart_total || ' rows)...');

    -- Oracle transactions start automatically, no explicit BEGIN needed
    WHILE cart_current < cart_total
    LOOP
        batch_end := cart_current + cart_batch_size;
        IF batch_end > cart_total THEN
            batch_end := cart_total;
        END IF;

        -- Insert batch using a CTE-style approach
        INSERT INTO testdb.cart (name, email, info, date_of_birth, created_at, is_active, login_count, balance)
        SELECT
            'cart-' || n,                                                    -- name
            'cart' || n || '@example.com',                                   -- email
            RPAD('This is about cart ' || n || '. ', 1000, 'X'),            -- info (40 repetitions ~1KB)
            SYSDATE - MOD(n, 10000),                                         -- date_of_birth, spread over ~27 years
            SYSTIMESTAMP,                                                    -- created_at
            CASE WHEN MOD(n, 2) = 0 THEN 1 ELSE 0 END,                      -- is_active alternating 1/0
            MOD(n, 100),                                                     -- login_count between 0-99
            CAST(MOD(n, 1000) + MOD(n, 100) / 100.0 AS NUMBER(10,2))        -- balance
        FROM (
            SELECT ROWNUM + cart_current AS n
            FROM dual
            CONNECT BY LEVEL <= (batch_end - cart_current)
        );

        cart_current := batch_end;

        -- Log progress after every batch
        DBMS_OUTPUT.PUT_LINE('Progress: ' || cart_current || '/' || cart_total || ' rows inserted into testdb.cart');

        -- Explicitly commit the current transaction
        COMMIT;

        -- Oracle automatically starts a new transaction after COMMIT
    END LOOP;

    DBMS_OUTPUT.PUT_LINE('Completed: ' || cart_current || ' rows inserted into testdb.cart');
END;
/

-- Verification
DECLARE
    cart_count NUMBER;
BEGIN
    SELECT COUNT(*) INTO cart_count FROM testdb.cart;
    DBMS_OUTPUT.PUT_LINE('Verification - testdb.cart: ' || cart_count || ' rows');
END;
/
