// Copyright 2025 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package oracledb

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
	defaultCacheKey = "max_scn"
	// defaultCheckpointCache can be configured by the user
	defaultCheckpointCache = "RPCN.CDC_CHECKPOINT_CACHE"
	// defaultStoredProcName schema is inferred from the provided checkpoint cache config
	// the stored procedure name cannot be configured by the user
	defaultStoredProcName = "CDC_CHECKPOINT_CACHE_UPDATE"
)

// allowedTableIdentifiers is used for validating cache table names
// Oracle identifiers: start with letter, up to 30 chars (128 in 12.2+), alphanumeric plus _ $ #
var allowedTableIdentifiers = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9_$#]{0,127}$`)

// cacheTable represents a formatted cache table name provided by the user configuration
type cacheTable struct{ schema, name string }

func (t cacheTable) String() string {
	return fmt.Sprintf("%s.%s", t.schema, t.name)
}

// checkpointCache is an Oracle specific cache created for the CDC component.
// We have a custom cache because the cache_sql component doesn't support Oracle due to its
// inability to support upserting (meaning it can't be expressed in the cache_sql configs).
type checkpointCache struct {
	db             *sql.DB
	cacheSetStmt   *sql.Stmt
	cacheTableName cacheTable

	log     *service.Logger
	shutSig *shutdown.Signaller
}

// newCheckpointCache create a new instance of the Oracle cache specific for CDC purposes.
// It initialises the state of the oracle based checkpoint cache, first creating the
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
		return nil, fmt.Errorf("invalid checkpoint cache table name: %w", err)
	}

	if db, err = sql.Open("oracle", connStr); err != nil {
		return nil, fmt.Errorf("connecting to oracle database for caching checkpoints: %w", err)
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
	if cacheSetStmt, err = db.PrepareContext(ctx, fmt.Sprintf("BEGIN %s.%s(:1, :2); END;", cacheTable.schema, defaultStoredProcName)); err != nil {
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

// Get a cache item, we only do this at start up, key can be ignored as we only ever store one entry
func (c *checkpointCache) Get(ctx context.Context, _ string) ([]byte, error) {
	if c.db == nil {
		return nil, fmt.Errorf("checkpoint cache not initialised for get operation: %w", service.ErrNotConnected)
	}

	var val []byte
	q := "SELECT cache_val FROM %s WHERE cache_key = :1"
	if err := c.db.QueryRowContext(ctx, fmt.Sprintf(q, c.cacheTableName.String()), defaultCacheKey).Scan(&val); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, service.ErrKeyNotFound
		}
		return nil, fmt.Errorf("querying checkpoint cache: %w", err)
	}
	return val, nil
}

// Set a cache item, specifying an optional TTL. It is okay for caches to
// ignore the ttl parameter if it isn't possible to implement. Key can be ignored as we only ever store one entry
func (c *checkpointCache) Set(ctx context.Context, _ string, value []byte, _ *time.Duration) error {
	if c.cacheSetStmt == nil {
		return errors.New("prepared statement for cache set not initialised")
	}
	if _, err := c.cacheSetStmt.ExecContext(ctx, defaultCacheKey, value); err != nil {
		return fmt.Errorf("writing to checkpoint cache: %w", err)
	}
	return nil
}

// Close closes the cache and any underlying connections
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
	// Check if table exists
	var count int
	checkQuery := `SELECT COUNT(*) FROM all_tables WHERE owner = :1 AND table_name = :2`
	if err := db.QueryRowContext(ctx, checkQuery, strings.ToUpper(tbl.schema), strings.ToUpper(tbl.name)).Scan(&count); err != nil {
		return false, fmt.Errorf("checking if table exists: %w", err)
	}

	if count > 0 {
		return false, nil // Table already exists
	}

	// Create table if it doesn't exist
	// cache_key length is based on default (fixed) cache key
	createQuery := fmt.Sprintf(`
		CREATE TABLE %s (
			cache_key VARCHAR2(10) NOT NULL PRIMARY KEY,
			cache_val VARCHAR2(100)
		)`, tbl.String())

	if _, err := db.ExecContext(ctx, createQuery); err != nil {
		return false, fmt.Errorf("creating table: %w", err)
	}

	return true, nil
}

func createUpsertStoredProc(ctx context.Context, db *sql.DB, cacheTable cacheTable) error {
	storedProcFullName := fmt.Sprintf("%s.%s", cacheTable.schema, defaultStoredProcName)
	tableName := cacheTable.String()

	// Drop procedure if it exists (Oracle doesn't have CREATE OR REPLACE for procedures in all versions)
	dropQuery := fmt.Sprintf(`
		BEGIN
			EXECUTE IMMEDIATE 'DROP PROCEDURE %s';
		EXCEPTION
			WHEN OTHERS THEN
				IF SQLCODE != -4043 THEN
					RAISE;
				END IF;
		END;`, storedProcFullName)

	if _, err := db.ExecContext(ctx, dropQuery); err != nil {
		// Ignore error if procedure doesn't exist
		return fmt.Errorf("dropping existing procedure: %w", err)
	}

	// Create the upsert procedure
	createQuery := fmt.Sprintf(`
		CREATE PROCEDURE %s (
			p_key IN VARCHAR2,
			p_value IN VARCHAR2
		)
		AS
			v_count NUMBER;
		BEGIN
			SELECT COUNT(*) INTO v_count FROM %s WHERE cache_key = p_key;

			IF v_count > 0 THEN
				UPDATE %s SET cache_val = p_value WHERE cache_key = p_key;
			ELSE
				INSERT INTO %s (cache_key, cache_val) VALUES (p_key, p_value);
			END IF;

			COMMIT;
		END;`, storedProcFullName, tableName, tableName, tableName)

	if _, err := db.ExecContext(ctx, createQuery); err != nil {
		return fmt.Errorf("creating procedure: %w", err)
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
	errInvalidSchemaLength          = errors.New("invalid schema length")
	errInvalidIdentifiedInTableName = errors.New("invalid identifier in table name")
	errInvalidTableFormat           = errors.New("table name must be in the format SCHEMA.TABLENAME")
)

// validateCacheTableName is called at start up and validates a table name including schema, e.g. "RPCN.PRODUCTS"
// Rules from Oracle identifier specifications
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
