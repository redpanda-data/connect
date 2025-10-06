package mssqlserver

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/Jeffail/shutdown"

	"github.com/redpanda-data/benthos/v4/public/service"
)

const (
	defaultCacheSchema = "dbo"
	defaultCacheKey    = "max_lsn"

	cacheTableName     = "__rp_connect_checkpoint_cache"
	cacheTableFullName = defaultCacheSchema + "." + cacheTableName

	// on the hot path
	storedProcName     = "__rp_connect_upsertCacheEntry"
	storedProcFullName = defaultCacheSchema + "." + storedProcName
)

// checkpointCache is a Microsoft SQL Server specific cache created for the CDC component.
// We have a custom cache because the cache_sql component doesn't support SQL Server due to its
// inability to support upserting (meaning it can't be expressed in the cache_sql configs).
type checkpointCache struct {
	db           *sql.DB
	cacheSetStmt *sql.Stmt

	shutSig *shutdown.Signaller
}

// newCheckpointCache create a new instance of the Microsoft SQL Server cache specific for CDC purposes.
// It initialises the state of the sql server based checkpoint cache, first creating the
// checkpoint cache table if it doesn't already exist then the checkpoint upsert stored procedure.
func newCheckpointCache(ctx context.Context, db *sql.DB) (*checkpointCache, error) {
	c := &checkpointCache{
		shutSig: shutdown.NewSignaller(),
	}

	if db == nil {
		return nil, errors.New("no database connection for checkpoint cache")
	}
	c.db = db

	if err := c.createCacheTable(ctx); err != nil {
		return nil, fmt.Errorf("creating checkpoint cache table: %w", err)
	}

	if err := c.createUpsertStoredProc(ctx); err != nil {
		return nil, fmt.Errorf("creating checkpoint cache write stored procedure: %w", err)
	}

	// use PreparedStatement for the Set operation to remove avoidable overhead
	stmt, err := c.db.PrepareContext(ctx, fmt.Sprintf("EXEC %s @Key=?, @Value=?", storedProcFullName))
	if err != nil {
		return nil, fmt.Errorf("preparing checkpoint cache statement: %w", err)
	}
	c.cacheSetStmt = stmt

	go func() {
		<-c.shutSig.HardStopChan()
		_ = c.db.Close()
		_ = c.cacheSetStmt.Close()
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
	if err := c.db.QueryRowContext(ctx, fmt.Sprintf(q, cacheTableFullName), defaultCacheKey).Scan(&val); err != nil {
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

func (c *checkpointCache) createCacheTable(ctx context.Context) error {
	// cache_key length is based on default (fixed) cache key
	q := `
	IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE schema_id = SCHEMA_ID('%s') AND name = '%s')
	BEGIN
		CREATE TABLE %s (
			cache_key varchar(7) NOT NULL PRIMARY KEY,
			cache_val varchar(100)
		);
	END;`
	if _, err := c.db.ExecContext(ctx, fmt.Sprintf(q, defaultCacheSchema, cacheTableName, cacheTableFullName)); err != nil {
		return err
	}
	return nil
}

func (c *checkpointCache) createUpsertStoredProc(ctx context.Context) error {
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
	if _, err := c.db.ExecContext(ctx, fmt.Sprintf(q, storedProcFullName, cacheTableFullName, cacheTableFullName, cacheTableFullName)); err != nil {
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
