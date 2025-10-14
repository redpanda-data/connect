package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	_ "github.com/microsoft/go-mssqldb"
)

func main() {
	port := "1433"
	// --- create database as master
	{
		connectionString := fmt.Sprintf("sqlserver://sa:YourStrong!Passw0rd@localhost:%s?database=%s&encrypt=disable", port, "master")
		var db *sql.DB
		var err error
		db, err = sql.Open("sqlserver", connectionString)
		mustBeNil(err)

		err = db.Ping()
		mustBeNil(err)

		fmt.Println("Creating 'testdb' database...")
		_, err = db.Exec(`
			IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = N'testdb')
			BEGIN
				CREATE DATABASE testdb;
				ALTER DATABASE testdb SET ALLOW_SNAPSHOT_ISOLATION ON;
			END;`)
		mustBeNil(err)
		db.Close()
	}

	// --- connect to database and enable CDC
	var (
		db  *sql.DB
		err error
	)
	{
		// Build connection string
		connectionString := fmt.Sprintf("sqlserver://sa:YourStrong!Passw0rd@localhost:%s?database=%s&encrypt=disable", port, "testdb")
		db, err = sql.Open("sqlserver", connectionString)
		mustBeNil(err)

		err = db.Ping()
		mustBeNil(err)

		// enable CDC on database
		fmt.Println("Enabling CDC on server...")
		_, err = db.Exec("EXEC sys.sp_cdc_enable_db;")
		mustBeNil(err)
	}

	// --- create tables and enable CDC on them
	{
		fmt.Println("Creating test tables 'dbo.users'...")

		testDB := &testDB{db}
		ctx := context.Background()
		err = testDB.createTableWithCDCEnabledIfNotExists(ctx, "dbo.users", `
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
		);`)
		mustBeNil(err)

		fmt.Println("Creating test tables 'dbo.products'...")
		err = testDB.createTableWithCDCEnabledIfNotExists(ctx, "dbo.products", `
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
		);`)
		mustBeNil(err)

		fmt.Println("Creating test tables 'dbo.cart'...")
		err = testDB.createTableWithCDCEnabledIfNotExists(ctx, "dbo.cart", `
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
		);`)
		mustBeNil(err)
	}

	// --- insert test data

	fmt.Println("Inserting test data into 'dbo.users'...")
	_, err = db.Exec(`
	WITH Numbers AS (
		SELECT TOP (1500000) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS n
		FROM sys.all_objects a
		CROSS JOIN sys.all_objects b
	)
	INSERT INTO dbo.users (name, surname, about, email, date_of_birth, join_date, created_at, is_active, login_count, balance)
	SELECT
		CONCAT('user-', n),										   -- name
		CONCAT('surname-', n),									   -- surname
		REPLICATE(CONCAT('This is about user ', n, '. '), 50000),  -- about ~1 MB
		CONCAT('user', n, '@example.com'),						   -- email
		DATEADD(DAY, -n % 10000, GETDATE()),					   -- date_of_birth, spread over ~27 years
		SYSUTCDATETIME(),										   -- join_date
		SYSUTCDATETIME(),										   -- created_at
		CASE WHEN n % 2 = 0 THEN 1 ELSE 0 END,				       -- is_active alternating 1/0
		n % 100,												   -- login_count between 0-99
		CAST((n % 1000) + RAND(CHECKSUM(NEWID())) * 100 AS DECIMAL(10,2)) -- balance
	FROM Numbers;
	`)
	mustBeNil(err)

	fmt.Println("Inserting test data into 'dbo.products'...")
	_, err = db.Exec(`
	WITH Numbers AS (
		SELECT TOP (1500000) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS n
		FROM sys.all_objects a
		CROSS JOIN sys.all_objects b
	)
	INSERT INTO dbo.products (name, info, description, email, date_added, join_date, created_at, is_active, basket_count, price)
	SELECT
		CONCAT('product-', n),										   -- name
		CONCAT('info-', n),									   -- info
		REPLICATE(CONCAT('This is about product ', n, '. '), 50000),  -- description ~1 MB
		CONCAT('help', n, '@example.com'),						   -- email
		DATEADD(DAY, -n % 10000, GETDATE()),					   -- date_added, spread over ~27 years
		SYSUTCDATETIME(),										   -- join_date
		SYSUTCDATETIME(),										   -- created_at
		CASE WHEN n % 2 = 0 THEN 1 ELSE 0 END,				       -- is_active alternating 1/0
		n % 100,												   -- basket_count between 0-99
		CAST((n % 1000) + RAND(CHECKSUM(NEWID())) * 100 AS DECIMAL(10,2)) -- price
	FROM Numbers;
	`)
	mustBeNil(err)

	fmt.Println("Inserting test data into 'dbo.cart'...")
	_, err = db.Exec(`
	WITH Numbers AS (
		SELECT TOP (10000000) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS n
		FROM sys.all_objects a
		CROSS JOIN sys.all_objects b
	)
	INSERT INTO dbo.cart (name, email, info, date_of_birth, created_at, is_active, login_count, balance)
	SELECT
		CONCAT('cart-', n),                                -- name
		CONCAT('cart', n, '@example.com'),                 -- email
		REPLICATE(CONCAT('This is about cart ', n, '. '), 40),  -- description 
		DATEADD(DAY, -n % 10000, GETDATE()),               -- date_of_birth, spread over ~27 years
		SYSUTCDATETIME(),                                  -- created_at
		CASE WHEN n % 2 = 0 THEN 1 ELSE 0 END,             -- is_active alternating 1/0
		n % 100,                                           -- login_count between 0-99
		CAST((n % 1000) + RAND(CHECKSUM(NEWID())) * 100 AS DECIMAL(10,2)) -- balance
	FROM Numbers;
	`)
	mustBeNil(err)

	fmt.Println("Done.")
}

func mustBeNil(err error) {
	if err != nil {
		panic(err)
	}
}

type testDB struct {
	*sql.DB
}

func (db *testDB) MustExec(query string, args ...any) {
	_, err := db.Exec(query, args...)
	mustBeNil(err)
}

func (db *testDB) createTableWithCDCEnabledIfNotExists(ctx context.Context, name, createTableQuery string, _ ...any) error {
	table := strings.Split(name, ".")
	if len(table) != 2 {
		return errors.New("schema must be specified")
	}
	schema := table[0]
	tableName := table[1]

	sql := `
	IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'rpcn')
	BEGIN
		EXEC('CREATE SCHEMA rpcn');
	END`
	if _, err := db.Exec(sql); err != nil {
		return err
	}

	enableSnapshot := `ALTER DATABASE testdb SET ALLOW_SNAPSHOT_ISOLATION ON;`
	enableCDC := fmt.Sprintf(`
		EXEC sys.sp_cdc_enable_table
		@source_schema = '%s',
		@source_name   = '%s',
		@role_name     = NULL;`, schema, tableName)
	sql = fmt.Sprintf(`
		IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = '%s' AND schema_id = SCHEMA_ID('%s'))
		BEGIN
			%s
			%s
			%s
		END;`, tableName, schema, createTableQuery, enableCDC, enableSnapshot)
	if _, err := db.Exec(sql); err != nil {
		return err
	}

	// wait for CDC table to be ready, this avoids time.sleeps
	for {
		var minLSN, maxLSN []byte
		// table isn't ready yet
		if err := db.QueryRowContext(ctx, "SELECT sys.fn_cdc_get_min_lsn(@p1)", name).Scan(&minLSN); err != nil {
			return err
		}
		// cdc agent still preparing
		if err := db.QueryRowContext(ctx, "SELECT sys.fn_cdc_get_max_lsn()").Scan(&maxLSN); err != nil {
			return err
		}
		if minLSN != nil && maxLSN != nil {
			break
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Second):
		}
	}
	return nil
}
