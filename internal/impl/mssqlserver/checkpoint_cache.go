// Copyright 2025 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

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
	// cache updates a single row so we use a fixed key
	defaultCacheKey = "max_lsn"
	// defaultCheckpointCache can be configured by the user
	defaultCheckpointCache = "rpcn.CdcCheckpointCache"
	// defaultStoredProcName schema is inferred from the provided checkpoint cache config
	// the stored procedure name cannot be configured by the user
	defaultStoredProcName = "CdcCheckpointCacheUpdate"
)

// allowedTableIdentifiers is used for validating cache table names
var allowedTableIdentifiers = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_$]{0,127}$`)

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

	if created, err := createCacheTable(ctx, db, cacheTable); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("creating checkpoint cache table '%s': %w", cacheTable.String(), err)
	} else if created {
		log.Infof("Created checkpoint cache table '%s'", cacheTable.String())
	} else {
		log.Infof("Found existing checkpoint cache table '%s'", cacheTable.String())
	}

	// this and its tests can eventually be deleted when we're happy users have migrated.
	if err := cacheTableMigration(ctx, db, cacheTable.String(), log); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("migrating checkpoint cache table '%s': %w", cacheTable.String(), err)
	}
	if err := createUpsertStoredProc(ctx, db, cacheTable); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("creating checkpoint cache write stored procedure: %w", err)
	}

	// create a prepared statement for calling the stored proc (created in same schema as cache table) during Set operations to remove avoidable overhead
	if cacheSetStmt, err = db.PrepareContext(ctx, fmt.Sprintf("EXEC [%s].[%s] @Key=?, @Value=?", cacheTable.schema, defaultStoredProcName)); err != nil {
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

// Get a cache item, we only do this at start up, key can be ignored as we only ever store one entry.
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
// ignore the ttl parameter if it isn't possible to implement. Key can be ignored as we only ever store one entry.
func (c *checkpointCache) Set(ctx context.Context, _ string, value []byte, _ *time.Duration) error {
	if c.cacheSetStmt == nil {
		return errors.New("prepared statement for cache set not initialised")
	}
	if _, err := c.cacheSetStmt.ExecContext(ctx, defaultCacheKey, value); err != nil {
		return fmt.Errorf("writing to checkpoint cache: %w", err)
	}
	return nil
}

// Close closes the cache and any underlying connections.
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
			cache_val varbinary(10)
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

// cacheTableMigration converts a legacy character-typed cache_val column to
// varbinary(10) in place. On-disk bytes are intact even for legacy rows, so
// the byte-level CONVERT recovers the true LSN rather than resetting it.
// No-op if the column is already binary.
//
// Guarded by sp_getapplock so pipelines sharing one checkpoint table don't
// race on the rename when starting concurrently.
func cacheTableMigration(ctx context.Context, db *sql.DB, tableName string, log *service.Logger) error {
	lockResource := tableName

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("beginning cache migration transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	var (
		lockRes int
		query   = "DECLARE @res INT; EXEC @res = sp_getapplock @Resource = ?, @LockMode = 'Exclusive'; SELECT @res;"
	)
	if err := tx.QueryRowContext(ctx, query, lockResource).Scan(&lockRes); err != nil {
		return fmt.Errorf("acquiring checkpoint cache migration lock: %w", err)
	}
	if lockRes < 0 {
		return fmt.Errorf("acquiring checkpoint cache migration lock (sp_getapplock returned %d)", lockRes)
	}

	var isChar bool
	checkQ := `
	SELECT CASE WHEN t.name IN ('varchar', 'char', 'nvarchar', 'nchar') THEN 1 ELSE 0 END
	FROM sys.columns c
	JOIN sys.types t ON t.user_type_id = c.user_type_id
	WHERE c.object_id = OBJECT_ID(?) AND c.name = 'cache_val';`
	if err := tx.QueryRowContext(ctx, checkQ, tableName).Scan(&isChar); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			// table/column not found is unexpected here since createCacheTable runs first,
			// but treat as nothing to migrate rather than failing startup.
			return tx.Commit()
		}
		return fmt.Errorf("inspecting checkpoint cache column type: %w", err)
	}
	if !isChar {
		// already migrated
		return tx.Commit()
	}

	log.Warnf("Migrating legacy character-typed cache_val column on checkpoint cache table '%s' to varbinary(10); recovered LSNs will be validated for length", tableName)

	migrationSteps := []string{
		fmt.Sprintf("ALTER TABLE %s ADD cache_val_bin varbinary(10) NULL;", tableName),
		fmt.Sprintf("UPDATE %s SET cache_val_bin = CONVERT(varbinary(10), cache_val);", tableName),
		fmt.Sprintf("ALTER TABLE %s DROP COLUMN cache_val;", tableName),
		fmt.Sprintf("EXEC sp_rename '%s.cache_val_bin', 'cache_val', 'COLUMN';", tableName),
	}
	for _, step := range migrationSteps {
		if _, err := tx.ExecContext(ctx, step); err != nil {
			return fmt.Errorf("converting cache_val column to varbinary: %w", err)
		}
	}

	validateQ := fmt.Sprintf(`UPDATE %s SET cache_val = NULL WHERE cache_val IS NOT NULL AND DATALENGTH(cache_val) <> 10;`, tableName)
	res, err := tx.ExecContext(ctx, validateQ)
	if err != nil {
		return fmt.Errorf("updating migrated cache_val length: %w", err)
	}
	invalidRows, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("validating migrated cache_val length: %w", err)
	}
	if invalidRows > 0 {
		log.Warnf("Checkpoint cache table '%s' had %d recovered LSN value(s) that failed length validation after migration and were reset; affected pipelines will resume from each table's tracked start LSN instead", tableName, invalidRows)
	}

	return tx.Commit()
}

func createUpsertStoredProc(ctx context.Context, db *sql.DB, cacheTable cacheTable) error {
	storedProcFullName := fmt.Sprintf("[%s].[%s]", cacheTable.schema, defaultStoredProcName)
	tableName := cacheTable.String()
	// key length is based on default (fixed) cache key
	q := `
	CREATE OR ALTER PROCEDURE %s
		@Key varchar(7),
		@Value varbinary(10)
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

// Add is unused.
func (*checkpointCache) Add(_ context.Context, _ string, _ []byte, _ *time.Duration) error {
	panic("not implemented")
}

// Delete is unused.
func (*checkpointCache) Delete(_ context.Context, _ string) error {
	panic("not implemented")
}

var (
	errEmptyTableName               = errors.New("empty table name")
	errInvalidTableLength           = errors.New("invalid table length")
	errInvalidSchemaLength          = errors.New("invalid schema length")
	errInvalidIdentifiedInTableName = errors.New("invalid identifier in table name")
	errInvalidTableFormat           = errors.New("table name must be in the format schema.tablename")
)

// validateCacheTableName is called at start up and validates a table name including schema, e.g. "dbo.products"
// Rules from https://learn.microsoft.com/en-us/sql/relational-databases/databases/database-identifiers
func validateCacheTableName(input string) (cacheTable, error) {
	if input == "" {
		return cacheTable{}, errEmptyTableName
	}

	parts := strings.Split(input, ".")
	if len(parts) != 2 {
		return cacheTable{}, errInvalidTableFormat
	}

	ct := cacheTable{schema: parts[0], name: parts[1]}

	if ct.schema == "" || len(ct.schema) > 128 {
		return cacheTable{}, errInvalidSchemaLength
	}
	if ct.name == "" || len(ct.name) > 128 {
		return cacheTable{}, errInvalidTableLength
	}
	if !allowedTableIdentifiers.MatchString(ct.schema) || !allowedTableIdentifiers.MatchString(ct.name) {
		return cacheTable{}, errInvalidIdentifiedInTableName
	}
	return ct, nil
}
