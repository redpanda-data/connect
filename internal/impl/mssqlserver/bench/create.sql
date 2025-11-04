-- MSSQL Server Benchmark Setup Script
-- This script creates the database, enables CDC, and creates tables
-- Connection: sqlserver://sa:YourStrong!Passw0rd@localhost:1433

-- ============================================================================
-- STAGE 1: Create Database
-- ============================================================================
PRINT '=== STAGE 1: Creating testdb database ==='
GO

USE master;
GO

IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = N'testdb')
BEGIN
    CREATE DATABASE testdb;
    ALTER DATABASE testdb SET ALLOW_SNAPSHOT_ISOLATION ON;
    PRINT 'Database testdb created successfully'
END
ELSE
BEGIN
    PRINT 'Database testdb already exists'
END
GO

-- ============================================================================
-- STAGE 2: Enable CDC on Database
-- ============================================================================
PRINT '=== STAGE 2: Enabling CDC on database ==='
GO

USE testdb;
GO

EXEC sys.sp_cdc_enable_db;
PRINT 'CDC enabled on database'
GO

-- ============================================================================
-- STAGE 3: Create Tables and Enable CDC
-- ============================================================================
PRINT '=== STAGE 3: Creating tables and enabling CDC ==='
GO

-- Create rpcn schema if needed
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'rpcn')
BEGIN
    EXEC('CREATE SCHEMA rpcn');
    PRINT 'Schema rpcn created'
END
GO

-- Create dbo.users table
PRINT 'Creating table dbo.users...'
GO

IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'users' AND schema_id = SCHEMA_ID('dbo'))
BEGIN
    CREATE TABLE dbo.users (
        id INT IDENTITY(1,1) PRIMARY KEY,
        name NVARCHAR(100) NOT NULL,
        surname NVARCHAR(100) NOT NULL,
        about NVARCHAR(MAX) NOT NULL,
        email NVARCHAR(255) NOT NULL,
        date_of_birth DATE NULL,
        join_date DATE NULL,
        created_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
        is_active BIT NOT NULL DEFAULT 1,
        login_count INT NOT NULL DEFAULT 0,
        balance DECIMAL(10,2) NOT NULL DEFAULT 0.00
    );
    
    EXEC sys.sp_cdc_enable_table
        @source_schema = 'dbo',
        @source_name   = 'users',
        @role_name     = NULL;
    
    PRINT 'Table dbo.users created and CDC enabled'
END
ELSE
BEGIN
    PRINT 'Table dbo.users already exists'
END
GO

-- Create dbo.products table
PRINT 'Creating table dbo.products...'
GO

IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'products' AND schema_id = SCHEMA_ID('dbo'))
BEGIN
    CREATE TABLE dbo.products (
        id INT IDENTITY(1,1) PRIMARY KEY,
        name NVARCHAR(100) NOT NULL,
        info NVARCHAR(100) NOT NULL,
        description NVARCHAR(MAX) NOT NULL,
        email NVARCHAR(255) NOT NULL,
        date_added DATE NULL,
        join_date DATE NULL,
        created_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
        is_active BIT NOT NULL DEFAULT 1,
        basket_count INT NOT NULL DEFAULT 0,
        price DECIMAL(10,2) NOT NULL DEFAULT 0.00
    );
    
    EXEC sys.sp_cdc_enable_table
        @source_schema = 'dbo',
        @source_name   = 'products',
        @role_name     = NULL;
    
    PRINT 'Table dbo.products created and CDC enabled'
END
ELSE
BEGIN
    PRINT 'Table dbo.products already exists'
END
GO

-- Create dbo.cart table
PRINT 'Creating table dbo.cart...'
GO

IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'cart' AND schema_id = SCHEMA_ID('dbo'))
BEGIN
    CREATE TABLE dbo.cart (
        id INT IDENTITY(1,1) PRIMARY KEY,
        name NVARCHAR(100) NOT NULL,
        info NVARCHAR(MAX) NOT NULL,
        email NVARCHAR(255) NOT NULL,
        date_of_birth DATE NULL,
        created_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
        is_active BIT NOT NULL DEFAULT 1,
        login_count INT NOT NULL DEFAULT 0,
        balance DECIMAL(10,2) NOT NULL DEFAULT 0.00
    );
    
    EXEC sys.sp_cdc_enable_table
        @source_schema = 'dbo',
        @source_name   = 'cart',
        @role_name     = NULL;
    
    PRINT 'Table dbo.cart created and CDC enabled'
END
ELSE
BEGIN
    PRINT 'Table dbo.cart already exists'
END
GO

IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'cart2' AND schema_id = SCHEMA_ID('dbo'))
BEGIN
CREATE TABLE dbo.cart2 (
                          id INT IDENTITY(1,1) PRIMARY KEY,
                          name NVARCHAR(100) NOT NULL,
                          info NVARCHAR(MAX) NOT NULL,
                          email NVARCHAR(255) NOT NULL,
                          date_of_birth DATE NULL,
                          created_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
                          is_active BIT NOT NULL DEFAULT 1,
                          login_count INT NOT NULL DEFAULT 0,
                          balance DECIMAL(10,2) NOT NULL DEFAULT 0.00
);

EXEC sys.sp_cdc_enable_table
        @source_schema = 'dbo',
        @source_name   = 'cart2',
        @role_name     = NULL;

    PRINT 'Table dbo.cart2 created and CDC enabled'
END
ELSE
BEGIN
    PRINT 'Table dbo.cart2 already exists'
END
GO
