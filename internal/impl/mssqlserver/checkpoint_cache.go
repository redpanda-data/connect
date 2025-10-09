package mssqlserver

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/Jeffail/shutdown"

	"github.com/redpanda-data/benthos/v4/public/service"
)

const (
	defaultCacheKey        = "max_lsn"
	defaultCheckpointCache = "rpcn.cdc_checkpoint_cache"
	storedProcName         = "CdcCheckpointUpsert"
)

// cacheTable represents a formatted cache table name provided by the user configuration
type cacheTable struct{ schema, name string }

func (t cacheTable) String() string {
	return fmt.Sprintf("%s.%s", t.schema, t.name)
}

// checkpointCache is a Microsoft SQL Server specific cache created for the CDC component.
// We have a custom cache because the cache_sql component doesn't support SQL Server due to its
// inability to support upserting (meaning it can't be expressed in the cache_sql configs).
type checkpointCache struct {
	db             *sql.DB
	cacheSetStmt   *sql.Stmt
	cacheTableName cacheTable

	log     *service.Logger
	shutSig *shutdown.Signaller
}

// newCheckpointCache create a new instance of the Microsoft SQL Server cache specific for CDC purposes.
// It initialises the state of the sql server based checkpoint cache, first creating the
// checkpoint cache table if it doesn't already exist then the checkpoint upsert stored procedure.
func newCheckpointCache(
	ctx context.Context,
	connStr string,
	cacheTableName string,
	log *service.Logger,
) (*checkpointCache, error) {
	var (
		err          error
		cacheTable   cacheTable
		db           *sql.DB
		cacheSetStmt *sql.Stmt
	)
	if connStr == "" {
		return nil, errors.New("no connection string provided")
	}

	if cacheTable, err = validateCacheTableName(cacheTableName); err != nil {
		return nil, fmt.Errorf("invalid checkpoint cache multipart table name: %w", err)
	}

	if db, err = sql.Open("mssql", connStr); err != nil {
		return nil, fmt.Errorf("connecting to microsoft sql server for caching checkpoints: %w", err)
	}

	if err := createUpsertStoredProc(ctx, db, cacheTable); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("creating checkpoint cache write stored procedure: %w", err)
	}

	if created, err := createCacheTable(ctx, db, cacheTable); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("creating checkpoint cache table '%s': %w", cacheTable.String(), err)
	} else if created {
		log.Infof("Created checkpoint cache table '%s'", cacheTable.String())
	} else {
		log.Infof("Found existing checkpoint cache table '%s'", cacheTable.String())
	}

	// create a prepared statement for calling the stored proc (created in same schema as cache table) during Set operations to remove avoidable overhead
	if cacheSetStmt, err = db.PrepareContext(ctx, fmt.Sprintf("EXEC [%s].[%s] @Key=?, @Value=?", cacheTable.schema, storedProcName)); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("preparing checkpoint cache statement: %w", err)
	}

	c := &checkpointCache{
		db:             db,
		cacheTableName: cacheTable,
		cacheSetStmt:   cacheSetStmt,

		log:     log,
		shutSig: shutdown.NewSignaller(),
	}

	go func() {
		<-c.shutSig.HardStopChan()
		_ = c.cacheSetStmt.Close()
		_ = c.db.Close()
		c.shutSig.TriggerHasStopped()
	}()

	return c, nil
}

// Get a cache item, we only do this at start up
func (c *checkpointCache) Get(ctx context.Context, _ string) ([]byte, error) {
	if c.db == nil {
		return nil, fmt.Errorf("checkpoint cache not initialised for get operation: %w", service.ErrNotConnected)
	}

	var val []byte
	q := "SELECT cache_val FROM %s WHERE cache_key = ?;"
	if err := c.db.QueryRowContext(ctx, fmt.Sprintf(q, c.cacheTableName.String()), defaultCacheKey).Scan(&val); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, service.ErrKeyNotFound
		}
		return nil, fmt.Errorf("querying checkpoint cache: %w", err)
	}
	return val, nil
}

// Set a cache item, specifying an optional TTL. It is okay for caches to
// ignore the ttl parameter if it isn't possible to implement.
func (c *checkpointCache) Set(ctx context.Context, _ string, value []byte, _ *time.Duration) error {
	if c.cacheSetStmt == nil {
		return errors.New("prepared statement for cache set not initialised")
	}
	if _, err := c.cacheSetStmt.ExecContext(ctx, defaultCacheKey, value); err != nil {
		return fmt.Errorf("writing to checkpoint cache: %w", err)
	}

	return nil
}

// Close closes the cache
func (c *checkpointCache) Close(ctx context.Context) error {
	c.shutSig.TriggerHardStop()
	select {
	case <-c.shutSig.HasStoppedChan():
	case <-ctx.Done():
		return ctx.Err()
	}
	return nil
}

func createCacheTable(ctx context.Context, db *sql.DB, tbl cacheTable) (bool, error) {
	// cache_key length is based on default (fixed) cache key
	q := `
	DECLARE @created BIT = 0;
	IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE schema_id = SCHEMA_ID('%s') AND name = '%s')
	BEGIN
		CREATE TABLE %s (
			cache_key varchar(7) NOT NULL PRIMARY KEY,
			cache_val varchar(100)
		);
		SET @created = 1;
	END;
	SELECT @created;`
	var created bool
	if err := db.QueryRowContext(ctx, fmt.Sprintf(q, tbl.schema, tbl.name, tbl.String())).Scan(&created); err != nil {
		return false, err
	}
	return created, nil
}

func createUpsertStoredProc(ctx context.Context, db *sql.DB, cacheTable cacheTable) error {
	storedProcFullName := fmt.Sprintf("[%s].[%s]", cacheTable.schema, storedProcName)
	tableName := cacheTable.String()
	// key length is based on default (fixed) cache key
	q := `
	CREATE OR ALTER PROCEDURE %s
		@Key varchar(7),
		@Value varchar(100)
	AS
	BEGIN
		SET NOCOUNT ON;
		IF EXISTS (SELECT 1 FROM %s WHERE cache_key = @Key)
			UPDATE %s SET cache_val = @Value WHERE cache_key = @Key;
		ELSE
			INSERT INTO %s (cache_key, cache_val) VALUES (@Key, @Value);
	END;`
	if _, err := db.ExecContext(ctx, fmt.Sprintf(q, storedProcFullName, tableName, tableName, tableName)); err != nil {
		return err
	}
	return nil
}

// Add is unused
func (*checkpointCache) Add(_ context.Context, _ string, _ []byte, _ *time.Duration) error {
	panic("not implemented")
}

// Delete is unused
func (*checkpointCache) Delete(_ context.Context, _ string) error {
	panic("not implemented")
}

var (
	errEmptyTableName               = errors.New("empty table name")
	errInvalidTableLength           = errors.New("invalid table length")
	errInvalidTableName             = errors.New("invalid table name")
	errInvalidIdentifiedInTableName = errors.New("invalid identifier in table name")
	errInvalidTableFormat           = errors.New("table name must be in the format schema.tablename")
)

// validateCacheTableName validates a table name including schema, e.g. "dbo.products"
func validateCacheTableName(input string) (cacheTable, error) {
	if input == "" {
		return cacheTable{}, errEmptyTableName
	}

	parts := strings.Split(input, ".")
	if len(parts) != 2 {
		return cacheTable{}, errInvalidTableFormat
	}

	ct := cacheTable{schema: parts[0], name: parts[1]}

	if len(ct.schema) < 1 {
		return cacheTable{}, errInvalidTableName
	}
	// "table_name can be a maximum of 128 characters..."
	// https://learn.microsoft.com/en-us/sql/t-sql/statements/create-table-transact-sql?view=sql-server-ver17&redirectedfrom=MSDN#table_name
	if len(ct.name) > 128 {
		return cacheTable{}, errInvalidTableLength
	}

	identifierRegex := regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)
	for _, part := range parts {
		if !identifierRegex.MatchString(part) {
			return cacheTable{}, errInvalidIdentifiedInTableName
		}
	}

	return ct, nil
}
