-- MSSQL Server Benchmark - Products Data
-- Connection: sqlserver://sa:YourStrong!Passw0rd@localhost:1433
-- Prerequisites: Run create.sql first

USE testdb;
GO

DECLARE @products_total INT = 150000;
PRINT CONCAT('Inserting test data into dbo.products (', @products_total, ' rows)...');
DECLARE @products_batch_size INT = 10000;
DECLARE @products_current INT = 0;

WHILE @products_current < @products_total
BEGIN
    DECLARE @products_batch_end INT = @products_current + @products_batch_size;
    IF @products_batch_end > @products_total
        SET @products_batch_end = @products_total;
    
    WITH Numbers AS (
        SELECT TOP (@products_batch_end - @products_current) 
            ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) + @products_current AS n
        FROM sys.all_objects a
        CROSS JOIN sys.all_objects b
    )
    INSERT INTO dbo.products WITH (TABLOCK) (name, info, description, email, date_added, join_date, created_at, is_active, basket_count, price)
    SELECT
        CONCAT('product-', n),                                        -- name
        CONCAT('info-', n),                                           -- info
        REPLICATE(CONCAT('This is about product ', n, '. '), 25000),  -- description ~500 KB
        CONCAT('help', n, '@example.com'),                            -- email
        DATEADD(DAY, -n % 10000, GETDATE()),                          -- date_added, spread over ~27 years
        SYSUTCDATETIME(),                                             -- join_date
        SYSUTCDATETIME(),                                             -- created_at
        CASE WHEN n % 2 = 0 THEN 1 ELSE 0 END,                        -- is_active alternating 1/0
        n % 100,                                                      -- basket_count between 0-99
        CAST((n % 1000) + (n % 100) / 100.0 AS DECIMAL(10,2)) -- price
    FROM Numbers;
    
    SET @products_current = @products_batch_end;
    
    -- Log progress after every batch
    PRINT CONCAT('Progress: ', @products_current, '/', @products_total, ' rows inserted into dbo.products');
END

PRINT CONCAT('Completed: ', @products_current, ' rows inserted into dbo.products');
GO

DECLARE @products_count INT;
SELECT @products_count = COUNT(*) FROM dbo.products;
PRINT CONCAT('Verification - dbo.products: ', @products_count, ' rows');
GO
