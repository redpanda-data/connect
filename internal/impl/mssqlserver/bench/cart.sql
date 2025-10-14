-- MSSQL Server Benchmark - Cart Data
-- Connection: sqlserver://sa:YourStrong!Passw0rd@localhost:1433
-- Prerequisites: Run create.sql first

USE testdb;
GO

DECLARE @cart_total INT = 10000000;
PRINT CONCAT('Inserting test data into dbo.cart (', @cart_total, ' rows)...');
DECLARE @cart_batch_size INT = 10000;
DECLARE @cart_current INT = 0;

-- Start the first transaction
BEGIN TRANSACTION;

WHILE @cart_current < @cart_total
BEGIN
    DECLARE @batch_end INT = @cart_current + @cart_batch_size;
    IF @batch_end > @cart_total
        SET @batch_end = @cart_total;
    
    WITH Numbers AS (
        SELECT TOP (@batch_end - @cart_current) 
            ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) + @cart_current AS n
        FROM sys.all_objects a
        CROSS JOIN sys.all_objects b
    )
    INSERT INTO dbo.cart WITH (TABLOCK) (name, email, info, date_of_birth, created_at, is_active, login_count, balance)
    SELECT
        CONCAT('cart-', n),                                    -- name
        CONCAT('cart', n, '@example.com'),                     -- email
        REPLICATE(CONCAT('This is about cart ', n, '. '), 40), -- description
        DATEADD(DAY, -n % 10000, GETDATE()),                   -- date_of_birth, spread over ~27 years
        SYSUTCDATETIME(),                                      -- created_at
        CASE WHEN n % 2 = 0 THEN 1 ELSE 0 END,                 -- is_active alternating 1/0
        n % 100,                                               -- login_count between 0-99
        CAST((n % 1000) + (n % 100) / 100.0 AS DECIMAL(10,2)) -- balance
    FROM Numbers;
    
    SET @cart_current = @batch_end;
    
    -- Log progress after every batch
    PRINT CONCAT('Progress: ', @cart_current, '/', @cart_total, ' rows inserted into dbo.cart');
    
    -- Explicitly commit the current transaction
    COMMIT;
    
    -- Start a new transaction for the next batch
    BEGIN TRANSACTION;
END

PRINT CONCAT('Completed: ', @cart_current, ' rows inserted into dbo.cart');
GO

DECLARE @cart_count INT;
SELECT @cart_count = COUNT(*) FROM dbo.cart;
PRINT CONCAT('Verification - dbo.cart: ', @cart_count, ' rows');
GO
