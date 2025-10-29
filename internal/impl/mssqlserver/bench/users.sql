-- MSSQL Server Benchmark - Users Data
-- Connection: sqlserver://sa:YourStrong!Passw0rd@localhost:1433
-- Prerequisites: Run create.sql first

USE testdb;
GO

DECLARE @users_total INT = 150000;
PRINT CONCAT('Inserting test data into dbo.users (', @users_total, ' rows)...');
DECLARE @users_batch_size INT = 10000;
DECLARE @users_current INT = 0;

WHILE @users_current < @users_total
BEGIN
    DECLARE @users_batch_end INT = @users_current + @users_batch_size;
    IF @users_batch_end > @users_total
        SET @users_batch_end = @users_total;
    
    WITH Numbers AS (
        SELECT TOP (@users_batch_end - @users_current) 
            ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) + @users_current AS n
        FROM sys.all_objects a
        CROSS JOIN sys.all_objects b
    )
    INSERT INTO dbo.users WITH (TABLOCK) (name, surname, about, email, date_of_birth, join_date, created_at, is_active, login_count, balance)
    SELECT
        CONCAT('user-', n),                                        -- name
        CONCAT('surname-', n),                                     -- surname
        REPLICATE(CONCAT('This is about user ', n, '. '), 25000),  -- about ~500 KB
        CONCAT('user', n, '@example.com'),                         -- email
        DATEADD(DAY, -n % 10000, GETDATE()),                       -- date_of_birth, spread over ~27 years
        SYSUTCDATETIME(),                                          -- join_date
        SYSUTCDATETIME(),                                          -- created_at
        CASE WHEN n % 2 = 0 THEN 1 ELSE 0 END,                     -- is_active alternating 1/0
        n % 100,                                                   -- login_count between 0-99
        CAST((n % 1000) + (n % 100) / 100.0 AS DECIMAL(10,2)) -- balance
    FROM Numbers;
    
    SET @users_current = @users_batch_end;
    
    -- Log progress after every batch
    PRINT CONCAT('Progress: ', @users_current, '/', @users_total, ' rows inserted into dbo.users');
END

PRINT CONCAT('Completed: ', @users_current, ' rows inserted into dbo.users');
GO

DECLARE @users_count INT;
SELECT @users_count = COUNT(*) FROM dbo.users;
PRINT CONCAT('Verification - dbo.users: ', @users_count, ' rows');
GO
