package oracledbtest

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	_ "github.com/sijms/go-ora/v2"

	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestDB wraps sql.DB with testing utilities for Oracle database integration tests.
// It provides helper methods for table creation, supplemental logging enablement, and assertions.
type TestDB struct {
	*sql.DB

	T *testing.T
}

// MustExec executes a SQL query and fails the test if an error occurs.
func (db *TestDB) MustExec(query string, args ...any) {
	_, err := db.Exec(query, args...)
	require.NoError(db.T, err)
}

// MustExecContext takes a context and executes a SQL query and fails the test if an error occurs.
func (db *TestDB) MustExecContext(ctx context.Context, query string, args ...any) {
	_, err := db.ExecContext(ctx, query, args...)
	require.NoError(db.T, err)
}

// MustEnableSupplementalLogging enables supplemental logging on the specified table.
// The fullTableName should be in format "schema.table" (e.g., "SYSTEM.all_data_types").
// If only a table name is provided, defaults to "SYSTEM" schema.
// This enables supplemental logging for all columns, which is required for CDC.
func (db *TestDB) MustEnableSupplementalLogging(ctx context.Context, fullTableName string) {
	db.T.Logf("Enabling supplemental logging for table %q", fullTableName)
	table := strings.Split(fullTableName, ".")
	if len(table) != 2 {
		table = []string{"SYSTEM", table[0]}
	}
	schema := strings.ToUpper(table[0])
	tableName := strings.ToUpper(table[1])

	// Enable supplemental logging for all columns on the table
	// This ensures all column values (before and after) are captured in redo logs
	query := fmt.Sprintf(`ALTER TABLE %s.%s ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS`, schema, tableName)

	_, err := db.ExecContext(ctx, query)
	require.NoError(db.T, err)

	db.T.Logf("Supplemental logging enabled for table %q", fullTableName)
}

// MustDisableSupplementalLogging disables supplemental logging on the specified table.
// The fullTableName should be in format "schema.table" (e.g., "SYSTEM.all_data_types").
// If only a table name is provided, defaults to "SYSTEM" schema.
func (db *TestDB) MustDisableSupplementalLogging(ctx context.Context, fullTableName string) {
	db.T.Logf("Disabling supplemental logging for table %q", fullTableName)
	table := strings.Split(fullTableName, ".")
	if len(table) != 2 {
		table = []string{"SYSTEM", table[0]}
	}
	schema := strings.ToUpper(table[0])
	tableName := strings.ToUpper(table[1])

	// Drop supplemental logging for all columns on the table
	query := fmt.Sprintf(`ALTER TABLE %s.%s DROP SUPPLEMENTAL LOG DATA (ALL) COLUMNS`, schema, tableName)

	_, err := db.ExecContext(ctx, query)
	require.NoError(db.T, err)

	db.T.Logf("Supplemental logging disabled for table %q", fullTableName)
}

// CreateTableWithSupplementalLoggingIfNotExists creates the given test tables ensuring supplemental logging is enabled.
func (db *TestDB) CreateTableWithSupplementalLoggingIfNotExists(ctx context.Context, fullTableName, createTableQuery string, _ ...any) error {
	// default to SYSTEM if not found
	table := strings.Split(fullTableName, ".")
	if len(table) != 2 {
		table = []string{"SYSTEM", table[0]}
	}
	schema := strings.ToUpper(table[0])
	tableName := strings.ToUpper(table[1])

	q := `
	DECLARE
		user_exists NUMBER;
	BEGIN
		SELECT COUNT(*) INTO user_exists FROM dba_users WHERE username = 'RPCN';
		IF user_exists = 0 THEN
			EXECUTE IMMEDIATE 'CREATE USER rpcn IDENTIFIED BY rpcn123';
			EXECUTE IMMEDIATE 'GRANT CONNECT, RESOURCE TO rpcn';
			EXECUTE IMMEDIATE 'GRANT UNLIMITED TABLESPACE TO rpcn';
		END IF;
	END;`
	if _, err := db.Exec(q); err != nil {
		return err
	}

	// Check if table exists using Oracle's user_tables view
	var count int
	err := db.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM all_tables WHERE owner = :1 AND table_name = :2",
		schema, tableName).Scan(&count)
	if err != nil {
		return err
	}

	// Only create table if it doesn't exist
	if count == 0 {
		// Create the table
		if _, err := db.ExecContext(ctx, createTableQuery); err != nil {
			return err
		}

		// Enable supplemental logging for all columns on the table
		enableSupplementalLogging := fmt.Sprintf(
			"ALTER TABLE %s.%s ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS",
			schema, tableName)
		if _, err := db.ExecContext(ctx, enableSupplementalLogging); err != nil {
			return err
		}
	}

	return nil
}

// SetupTestWithOracleDBVersion starts an Oracle XE Docker container with the specified version,
// enables supplemental logging for CDC, and returns the connection string and TestDB wrapper.
// The container is automatically cleaned up when the test completes.
func SetupTestWithOracleDBVersion(t *testing.T, version string) (string, *TestDB) {
	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	pool.MaxWait = time.Minute * 3 // Oracle takes longer to start
	// Oracle XE specific environment variables
	resource, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository: "container-registry.oracle.com/database/express",
		Tag:        version,
		Env: []string{
			"ORACLE_PWD=YourPassword123",
		},
		Cmd:          []string{},
		ExposedPorts: []string{"1521/tcp"},
	}, func(config *docker.HostConfig) {
		// set AutoRemove to true so that stopped container goes away by itself
		config.AutoRemove = true
		config.RestartPolicy = docker.RestartPolicy{
			Name: "no",
		}
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, pool.Purge(resource))
	})

	port := resource.GetPort("1521/tcp")

	var (
		db                  *sql.DB
		pdbConnectionString string
	)

	err = pool.Retry(func() error {
		var err error

		connStr := fmt.Sprintf("oracle://system:YourPassword123@localhost:%s/XE", port)
		dbConn, err := sql.Open("oracle", connStr)
		if err != nil {
			return err
		}
		defer dbConn.Close()

		dbConn.SetMaxOpenConns(10)
		dbConn.SetMaxIdleConns(5)
		dbConn.SetConnMaxLifetime(time.Minute * 5)

		if err = dbConn.Ping(); err != nil {
			return err
		}

		_, err = dbConn.Exec("ALTER DATABASE ADD SUPPLEMENTAL LOG DATA")
		assert.NoError(t, err)

		// Enable minimal supplemental logging for primary keys at CDB level
		_, err = dbConn.Exec("ALTER DATABASE ADD SUPPLEMENTAL LOG DATA (PRIMARY KEY) COLUMNS")
		assert.NoError(t, err)

		// Now connect to the PDB (XEPDB1) for application use
		pdbConnectionString = fmt.Sprintf("oracle://system:YourPassword123@localhost:%s/XEPDB1", port)
		db, err = sql.Open("oracle", pdbConnectionString)
		if err != nil {
			return err
		}

		db.SetMaxOpenConns(10)
		db.SetMaxIdleConns(5)
		db.SetConnMaxLifetime(time.Minute * 5)

		if err = db.Ping(); err != nil {
			return err
		}

		return nil
	})
	require.NoError(t, err)

	sql := `
	DECLARE
		user_exists NUMBER;
	BEGIN
		SELECT COUNT(*) INTO user_exists FROM dba_users WHERE username = 'TESTDB';
		IF user_exists = 0 THEN
			EXECUTE IMMEDIATE 'CREATE USER testdb IDENTIFIED BY testdb123';
			EXECUTE IMMEDIATE 'GRANT CONNECT, RESOURCE, DBA TO testdb';
			EXECUTE IMMEDIATE 'GRANT UNLIMITED TABLESPACE TO testdb';
		END IF;
	END;`

	_, err = db.ExecContext(t.Context(), sql)
	assert.NoError(t, err, "Creating 'testdb' schema for testing across multiple schemas")

	sql = `
	DECLARE
		user_exists NUMBER;
	BEGIN
		SELECT COUNT(*) INTO user_exists FROM dba_users WHERE username = 'TESTDB2';
		IF user_exists = 0 THEN
			EXECUTE IMMEDIATE 'CREATE USER testdb2 IDENTIFIED BY testdb2123';
			EXECUTE IMMEDIATE 'GRANT CONNECT, RESOURCE, DBA TO testdb2';
			EXECUTE IMMEDIATE 'GRANT UNLIMITED TABLESPACE TO testdb2';
		END IF;
	END;`

	_, err = db.ExecContext(t.Context(), sql)
	assert.NoError(t, err, "Creating 'testdb2' schema for testing across multiple schemas")

	t.Cleanup(func() {
		assert.NoError(t, db.Close())
	})
	return pdbConnectionString, &TestDB{db, t}
}
