-- Oracle Database Benchmark - Products Data
-- Connection: oracle://system:YourPassword123@localhost:1521/XE
-- Prerequisites: Run create.sql first

-- Enable output for debugging
SET SERVEROUTPUT ON;
-- Don't echo the old/new lines when &products_total is substituted
SET VERIFY OFF;

-- Switch to testdb schema
ALTER SESSION SET CURRENT_SCHEMA = testdb;
/

DECLARE
    -- products_total is defined by the sqlcl:pdb:data:products task (TOTAL=...)
    products_total NUMBER := &products_total;
    products_batch_size NUMBER := 10000;
    products_current NUMBER := 0;
    products_batch_end NUMBER;
BEGIN
    DBMS_OUTPUT.PUT_LINE('Inserting test data into testdb.products (' || products_total || ' rows)...');

    WHILE products_current < products_total
    LOOP
        products_batch_end := products_current + products_batch_size;
        IF products_batch_end > products_total THEN
            products_batch_end := products_total;
        END IF;

        -- Insert batch using a CTE-style approach
        INSERT INTO testdb.products (name, info, inlinedesc, outoflinedesc, email, date_added, join_date, created_at, is_active, basket_count, price)
        SELECT
            'product-' || n,                                                 -- name
            'info-' || n,                                                    -- info
            RPAD('This is inlined' || n || '. ', 5, 'X'),                    -- inlinedesc: 5 characters, as RPAD also truncates
            RPAD('This out out of lined' || n || '. ', 100000, 'X'),         -- outoflinedesc: 4,000 characters (~8 KB as NCLOB, so stored out of line), as RPAD in SQL caps at 4,000
            'help' || n || '@example.com',                                   -- email
            SYSDATE - MOD(n, 10000),                                         -- date_added, spread over ~27 years
            SYSTIMESTAMP,                                                    -- join_date
            SYSTIMESTAMP,                                                    -- created_at
            CASE WHEN MOD(n, 2) = 0 THEN 1 ELSE 0 END,                      -- is_active alternating 1/0
            MOD(n, 100),                                                     -- basket_count between 0-99
            CAST(MOD(n, 1000) + MOD(n, 100) / 100.0 AS NUMBER(10,2))        -- price
        FROM (
            SELECT ROWNUM + products_current AS n
            FROM dual
            CONNECT BY LEVEL <= (products_batch_end - products_current)
        );

        COMMIT;

        products_current := products_batch_end;

        -- Log progress after every batch
        DBMS_OUTPUT.PUT_LINE('Progress: ' || products_current || '/' || products_total || ' rows inserted into testdb.products');
    END LOOP;

    DBMS_OUTPUT.PUT_LINE('Completed: ' || products_current || ' rows inserted into testdb.products');
END;
/

-- Gather optimizer statistics so the snapshot can split testdb.products into parallel chunks
BEGIN
    DBMS_STATS.GATHER_TABLE_STATS(ownname => 'TESTDB', tabname => 'PRODUCTS');
    DBMS_OUTPUT.PUT_LINE('Gathered optimizer statistics for testdb.products');
END;
/

-- Verification
DECLARE
    products_count NUMBER;
BEGIN
    SELECT COUNT(*) INTO products_count FROM testdb.products;
    DBMS_OUTPUT.PUT_LINE('Verification - testdb.products: ' || products_count || ' rows');
END;
/
