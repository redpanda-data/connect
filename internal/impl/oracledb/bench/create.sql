-- Oracle Database Benchmark Setup Script
-- This script creates the user/schema, enables supplemental logging, and creates tables
-- Connection: oracle://system:oracle@localhost:1521/XEPDB1

-- ============================================================================
-- STAGE 1: Create User/Schema
-- ============================================================================
BEGIN
    DBMS_OUTPUT.PUT_LINE('=== STAGE 1: Creating testdb user ===');
END;
/

DECLARE
    user_exists NUMBER;
BEGIN
    SELECT COUNT(*) INTO user_exists FROM dba_users WHERE username = 'TESTDB';

    IF user_exists = 0 THEN
        EXECUTE IMMEDIATE 'CREATE USER testdb IDENTIFIED BY testdb123';
        EXECUTE IMMEDIATE 'GRANT CONNECT, RESOURCE, DBA TO testdb';
        EXECUTE IMMEDIATE 'GRANT UNLIMITED TABLESPACE TO testdb';
        DBMS_OUTPUT.PUT_LINE('User testdb created successfully');
    ELSE
        DBMS_OUTPUT.PUT_LINE('User testdb already exists');
    END IF;
END;
/

-- ============================================================================
-- STAGE 2: Enable Supplemental Logging for CDC
-- ============================================================================
BEGIN
    DBMS_OUTPUT.PUT_LINE('=== STAGE 2: Enabling supplemental logging ===');
END;
/

-- Enable minimal supplemental logging at database level
ALTER DATABASE ADD SUPPLEMENTAL LOG DATA;

-- Enable primary key and unique key supplemental logging
ALTER DATABASE ADD SUPPLEMENTAL LOG DATA (PRIMARY KEY, UNIQUE) COLUMNS;

BEGIN
    DBMS_OUTPUT.PUT_LINE('Supplemental logging enabled');
END;
/

-- ============================================================================
-- STAGE 3: Create Tables and Enable Supplemental Logging
-- ============================================================================
BEGIN
    DBMS_OUTPUT.PUT_LINE('=== STAGE 3: Creating tables and enabling CDC ===');
END;
/

-- Switch to testdb user context
ALTER SESSION SET CURRENT_SCHEMA = testdb;
/

-- Create rpcn user if needed (Oracle uses users/schemas interchangeably)
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

-- Create testdb.users table
BEGIN
    DBMS_OUTPUT.PUT_LINE('Creating table testdb.users...');
END;
/

DECLARE
    table_exists NUMBER;
BEGIN
    SELECT COUNT(*) INTO table_exists
    FROM user_tables
    WHERE table_name = 'USERS';

    IF table_exists = 0 THEN
        EXECUTE IMMEDIATE '
            CREATE TABLE testdb.users (
                id NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                name NVARCHAR2(100) NOT NULL,
                surname NVARCHAR2(100) NOT NULL,
                about NCLOB NOT NULL,
                email NVARCHAR2(255) NOT NULL,
                date_of_birth DATE,
                join_date DATE,
                created_at TIMESTAMP DEFAULT SYSTIMESTAMP NOT NULL,
                is_active NUMBER(1) DEFAULT 1 NOT NULL,
                login_count NUMBER DEFAULT 0 NOT NULL,
                balance NUMBER(10,2) DEFAULT 0.00 NOT NULL
            )';

        -- Enable supplemental logging for this table
        EXECUTE IMMEDIATE 'ALTER TABLE testdb.users ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS';

        DBMS_OUTPUT.PUT_LINE('Table testdb.users created and supplemental logging enabled');
    ELSE
        DBMS_OUTPUT.PUT_LINE('Table testdb.users already exists');
    END IF;
END;
/

-- Create testdb.products table
BEGIN
    DBMS_OUTPUT.PUT_LINE('Creating table testdb.products...');
END;
/

DECLARE
    table_exists NUMBER;
BEGIN
    SELECT COUNT(*) INTO table_exists
    FROM user_tables
    WHERE table_name = 'PRODUCTS';

    IF table_exists = 0 THEN
        EXECUTE IMMEDIATE '
            CREATE TABLE testdb.products (
                id NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                name NVARCHAR2(100) NOT NULL,
                info NVARCHAR2(100) NOT NULL,
                description NCLOB NOT NULL,
                email NVARCHAR2(255) NOT NULL,
                date_added DATE,
                join_date DATE,
                created_at TIMESTAMP DEFAULT SYSTIMESTAMP NOT NULL,
                is_active NUMBER(1) DEFAULT 1 NOT NULL,
                basket_count NUMBER DEFAULT 0 NOT NULL,
                price NUMBER(10,2) DEFAULT 0.00 NOT NULL
            )';

        -- Enable supplemental logging for this table
        EXECUTE IMMEDIATE 'ALTER TABLE testdb.products ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS';

        DBMS_OUTPUT.PUT_LINE('Table testdb.products created and supplemental logging enabled');
    ELSE
        DBMS_OUTPUT.PUT_LINE('Table testdb.products already exists');
    END IF;
END;
/

-- Create testdb.cart table
BEGIN
    DBMS_OUTPUT.PUT_LINE('Creating table testdb.cart...');
END;
/

DECLARE
    table_exists NUMBER;
BEGIN
    SELECT COUNT(*) INTO table_exists
    FROM user_tables
    WHERE table_name = 'CART';

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

        -- Enable supplemental logging for this table
        EXECUTE IMMEDIATE 'ALTER TABLE testdb.cart ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS';

        DBMS_OUTPUT.PUT_LINE('Table testdb.cart created and supplemental logging enabled');
    ELSE
        DBMS_OUTPUT.PUT_LINE('Table testdb.cart already exists');
    END IF;
END;
/

-- Create testdb.cart2 table
BEGIN
    DBMS_OUTPUT.PUT_LINE('Creating table testdb.cart2...');
END;
/

DECLARE
    table_exists NUMBER;
BEGIN
    SELECT COUNT(*) INTO table_exists
    FROM user_tables
    WHERE table_name = 'CART2';

    IF table_exists = 0 THEN
        EXECUTE IMMEDIATE '
            CREATE TABLE testdb.cart2 (
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

        -- Enable supplemental logging for this table
        EXECUTE IMMEDIATE 'ALTER TABLE testdb.cart2 ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS';

        DBMS_OUTPUT.PUT_LINE('Table testdb.cart2 created and supplemental logging enabled');
    ELSE
        DBMS_OUTPUT.PUT_LINE('Table testdb.cart2 already exists');
    END IF;
END;
/
