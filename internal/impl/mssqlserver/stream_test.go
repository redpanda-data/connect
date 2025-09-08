package mssqlserver

import (
	"database/sql"
	"fmt"
	"testing"

	_ "github.com/microsoft/go-mssqldb"

	"github.com/stretchr/testify/require"
)

// Test_ManualTesting_AddTestDataWithUniqueLSN adds data to an existing table and ensures each change has its own LSN
func Test_ManualTesting_AddTestDataWithUniqueLSN(t *testing.T) {
	t.Skip()
	port := "1433"
	connectionString := fmt.Sprintf("sqlserver://sa:YourStrong!Passw0rd@localhost:%s?database=%s&encrypt=disable", port, "mydb")

	db, err := sql.Open("mssql", connectionString)
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec(`
DECLARE @i INT = 1;
WHILE @i <= 50000
BEGIN
    INSERT INTO products (id, name)
    VALUES (@i, CONCAT('product-', @i));
    SET @i += 1;
END
	`)
	require.NoError(t, err)

	_, err = db.Exec(`
DECLARE @i INT = 1;
WHILE @i <= 50000
BEGIN
    INSERT INTO users (id, name)
    VALUES (@i, CONCAT('product-', @i));
    SET @i += 1;
END
	`)
	require.NoError(t, err)
}

// Test_ManualTesting_AddTestDataWithSameLSN adds lots of data to an existing table but all in one transaction meaning it'll share the same LSN
func Test_ManualTesting_AddTestDataWithSameLSN(t *testing.T) {
	t.Skip()
	port := "1433"
	connectionString := fmt.Sprintf("sqlserver://sa:YourStrong!Passw0rd@localhost:%s?database=%s&encrypt=disable", port, "mydb")

	db, err := sql.Open("mssql", connectionString)
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec(`
		WITH Numbers AS (
			SELECT TOP (1000000) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS n
			FROM sys.all_objects a
			CROSS JOIN sys.all_objects b
		)
		INSERT INTO users (id, name)
		SELECT n, CONCAT('user-', n) FROM Numbers;
	`)
	require.NoError(t, err)

	_, err = db.Exec(`
		WITH Numbers AS (
			SELECT TOP (1000000) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS n
			FROM sys.all_objects a
			CROSS JOIN sys.all_objects b
		)
		INSERT INTO products (id, name)
		SELECT n, CONCAT('product-', n) FROM Numbers;
	`)
	require.NoError(t, err)
}

func BenchmarkStreamingCDCChanges(b *testing.B) {
	b.Skip()
	port := "1433"
	connectionString := fmt.Sprintf("sqlserver://sa:YourStrong!Passw0rd@localhost:%s?database=%s&encrypt=disable", port, "mydb")
	db, err := sql.Open("mssql", connectionString)
	require.NoError(b, err)
	defer db.Close()

	ctStream := &changeTableStream{}

	err = ctStream.verifyChangeTables(b.Context(), db, []string{"users", "products"})
	require.NoError(b, err)

	b.ReportAllocs()
	// Reset timer to exclude setup time
	for b.Loop() {
		err := ctStream.readChangeTables(b.Context(), db, func(c *change) ([]byte, error) {
			fmt.Printf("LSN=%x, CommandID=%d, SeqVal=%x, op=%d table=%s cols=%d\n", c.startLSN, c.commandID, c.seqVal, c.operation, c.table, len(c.columns))
			return c.startLSN, nil
		})
		require.NoError(b, err)
	}
}
