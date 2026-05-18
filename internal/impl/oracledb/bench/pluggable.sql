-- ============================================================================
-- STAGE 1: Create users/schemas
-- ============================================================================
BEGIN
    DBMS_OUTPUT.PUT_LINE('=== STAGE 1: Creating users in TESTPDB ===');
END;
/

-- TESTDB: application schema
DECLARE
    user_exists NUMBER;
BEGIN
    SELECT COUNT(*) INTO user_exists FROM dba_users WHERE username = 'TESTDB';

    IF user_exists = 0 THEN
        EXECUTE IMMEDIATE 'CREATE USER testdb IDENTIFIED BY testdb123';
        EXECUTE IMMEDIATE 'GRANT CONNECT, RESOURCE, DBA TO testdb';
        EXECUTE IMMEDIATE 'GRANT UNLIMITED TABLESPACE TO testdb';
        EXECUTE IMMEDIATE 'GRANT LOGMINING TO testdb';
        DBMS_OUTPUT.PUT_LINE('User testdb created');
    ELSE
        DBMS_OUTPUT.PUT_LINE('User testdb already exists');
    END IF;
END;
/

-- RPCN: checkpoint cache schema
DECLARE
    user_exists NUMBER;
BEGIN
    SELECT COUNT(*) INTO user_exists FROM dba_users WHERE username = 'RPCN';

    IF user_exists = 0 THEN
        EXECUTE IMMEDIATE 'CREATE USER rpcn IDENTIFIED BY rpcn123';
        EXECUTE IMMEDIATE 'GRANT CONNECT, RESOURCE TO rpcn';
        EXECUTE IMMEDIATE 'GRANT UNLIMITED TABLESPACE TO rpcn';
        DBMS_OUTPUT.PUT_LINE('User rpcn created');
    ELSE
        DBMS_OUTPUT.PUT_LINE('User rpcn already exists');
    END IF;
END;
/

-- ============================================================================
-- STAGE 2: Enable supplemental logging
-- ============================================================================
BEGIN
    DBMS_OUTPUT.PUT_LINE('=== STAGE 2: Enabling supplemental logging in TESTPDB ===');
END;
/

ALTER DATABASE ADD SUPPLEMENTAL LOG DATA;
/

ALTER DATABASE ADD SUPPLEMENTAL LOG DATA (PRIMARY KEY, UNIQUE) COLUMNS;
/

BEGIN
    DBMS_OUTPUT.PUT_LINE('Supplemental logging enabled');
END;
/

-- ============================================================================
-- STAGE 3: Create tables
-- ============================================================================
BEGIN
    DBMS_OUTPUT.PUT_LINE('=== STAGE 3: Creating tables ===');
END;
/

ALTER SESSION SET CURRENT_SCHEMA = testdb;
/

DECLARE
    table_exists NUMBER;
BEGIN
    SELECT COUNT(*) INTO table_exists FROM user_tables WHERE table_name = 'USERS';

    IF table_exists = 0 THEN
        EXECUTE IMMEDIATE '
            CREATE TABLE testdb.users (
                id NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                name NVARCHAR2(100) NOT NULL,
                surname NVARCHAR2(100) NOT NULL,
                email NVARCHAR2(255) NOT NULL,
                date_of_birth DATE,
                join_date DATE,
                created_at TIMESTAMP DEFAULT SYSTIMESTAMP NOT NULL,
                is_active NUMBER(1) DEFAULT 1 NOT NULL,
                login_count NUMBER DEFAULT 0 NOT NULL,
                balance NUMBER(10,2) DEFAULT 0.00 NOT NULL
            )';
        EXECUTE IMMEDIATE 'ALTER TABLE testdb.users ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS';
        DBMS_OUTPUT.PUT_LINE('Table testdb.users created');
    ELSE
        DBMS_OUTPUT.PUT_LINE('Table testdb.users already exists');
    END IF;
END;
/

DECLARE
    table_exists NUMBER;
BEGIN
    SELECT COUNT(*) INTO table_exists FROM user_tables WHERE table_name = 'PRODUCTS';

    IF table_exists = 0 THEN
        EXECUTE IMMEDIATE '
            CREATE TABLE testdb.products (
                id NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                name NVARCHAR2(100) NOT NULL,
                info NVARCHAR2(100) NOT NULL,
                inlinedesc NCLOB NOT NULL,
                outoflinedesc NCLOB NOT NULL,
                email NVARCHAR2(255) NOT NULL,
                date_added DATE,
                join_date DATE,
                created_at TIMESTAMP DEFAULT SYSTIMESTAMP NOT NULL,
                is_active NUMBER(1) DEFAULT 1 NOT NULL,
                basket_count NUMBER DEFAULT 0 NOT NULL,
                price NUMBER(10,2) DEFAULT 0.00 NOT NULL
            )';
        EXECUTE IMMEDIATE 'ALTER TABLE testdb.products ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS';
        DBMS_OUTPUT.PUT_LINE('Table testdb.products created');
    ELSE
        DBMS_OUTPUT.PUT_LINE('Table testdb.products already exists');
    END IF;
END;
/

DECLARE
    table_exists NUMBER;
BEGIN
    SELECT COUNT(*) INTO table_exists FROM user_tables WHERE table_name = 'CART';

    IF table_exists = 0 THEN
        EXECUTE IMMEDIATE '
            CREATE TABLE testdb.cart (
                id NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                name NVARCHAR2(100) NOT NULL,
                info NCLOB NOT NULL,
                email NVARCHAR2(255) NOT NULL,
                date_of_birth DATE,
                created_at TIMESTAMP DEFAULT SYSTIMESTAMP NOT NULL,
                is_active NUMBER(1) DEFAULT 1 NOT NULL,
                login_count NUMBER DEFAULT 0 NOT NULL,
                balance NUMBER(10,2) DEFAULT 0.00 NOT NULL
            )';
        EXECUTE IMMEDIATE 'ALTER TABLE testdb.cart ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS';
        DBMS_OUTPUT.PUT_LINE('Table testdb.cart created');
    ELSE
        DBMS_OUTPUT.PUT_LINE('Table testdb.cart already exists');
    END IF;
END;
/

BEGIN
    DBMS_OUTPUT.PUT_LINE('');
    DBMS_OUTPUT.PUT_LINE('=== TESTPDB setup complete ===');
    DBMS_OUTPUT.PUT_LINE('Connection string : oracle://testdb:testdb123@localhost:1521/TESTPDB');
    DBMS_OUTPUT.PUT_LINE('pdb_name config   : TESTPDB');
    DBMS_OUTPUT.PUT_LINE('Tables            : TESTDB.USERS, TESTDB.PRODUCTS, TESTDB.CART');
END;
/
