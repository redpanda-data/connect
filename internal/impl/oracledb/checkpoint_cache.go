// Copyright 2026 Redpanda Data, Inc.
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
	goora "github.com/sijms/go-ora/v2/network"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/connect/v4/internal/impl/oracledb/replication"
	"github.com/redpanda-data/connect/v4/internal/sqlredact"
)

const (
	// defaultCheckpointCache can be configured by the user
	defaultCheckpointCache = "RPCN.CDC_CHECKPOINT_CACHE"
	// defaultStoredProcName is the deprecated checkpoint write procedure, in the
	// schema of the checkpoint cache table. Its name cannot be configured. Only
	// a user that cannot run the MERGE of mergeWrite uses it.
	defaultStoredProcName = "CDC_CHECKPOINT_CACHE_UPDATE"
	// errCodeNameAlreadyUsed is ORA-00955: "name is already used by an existing object".
	errCodeNameAlreadyUsed = 955
	// checkpointCacheKeyLimit specifies the maximum length of the checkpoint cache key
	checkpointCacheKeyLimit = 128
	// checkpointMergeProbeKey is the key that the start up MERGE probe
	// inserts and rolls back. No pipeline uses it.
	checkpointMergeProbeKey = "__rpcn_checkpoint_merge_probe"
	// checkpointMergeProbeTimeout limits the probe. Concurrent probes of one
	// table wait on the probe key until the first one rolls back. A probe
	// blocked by a dead session times out, and Connect tries again.
	checkpointMergeProbeTimeout = 10 * time.Second
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
//
// Set upserts with a MERGE statement. A user without the INSERT and UPDATE
// privileges on the table cannot run it. Older versions wrote with the stored
// procedure <schema>.CDC_CHECKPOINT_CACHE_UPDATE, which runs with the rights of
// its owner, so such a user worked. For these users, Set falls back to the
// procedure until a future version removes it.
type checkpointCache struct {
	db             *sql.DB
	cacheSetStmt   *sql.Stmt
	cacheTableName cacheTable

	log     *service.Logger
	shutSig *shutdown.Signaller
}

// newCheckpointCache create a new instance of the Oracle cache specific for CDC purposes.
// It initialises the state of the oracle based checkpoint cache, creating the checkpoint cache
// table if it doesn't already exist, and selects how Set writes (see checkpointCache).
func newCheckpointCache(
	ctx context.Context,
	connStr string,
	cacheTableName string,
	cacheKey string,
	metrics *service.Metrics,
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

	if err = validateCacheKey(cacheKey); err != nil {
		return nil, fmt.Errorf("invalid checkpoint cache key: %w", err)
	}

	if cacheTable, err = validateCacheTableName(cacheTableName); err != nil {
		return nil, fmt.Errorf("invalid checkpoint cache table name: %w", err)
	}

	if db, err = sqlredact.Open("oracle", connStr); err != nil {
		return nil, fmt.Errorf("connecting to oracle database for caching checkpoints: %w", err)
	}

	if created, err := createCacheTable(ctx, db, cacheTable, cacheKey, log); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("creating checkpoint cache table '%s': %w", cacheTable.String(), err)
	} else if created {
		log.Infof("Created checkpoint cache table '%s'", cacheTable.String())
	} else {
		log.Infof("Found existing checkpoint cache table '%s'", cacheTable.String())
	}

	setQuery, err := selectCheckpointWrite(ctx, db, cacheTable, metrics, log)
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	// The connection has no open transaction, so go-ora commits every MERGE (autocommit is on by default).
	// The procedure commits itself.
	if cacheSetStmt, err = db.PrepareContext(ctx, setQuery); err != nil {
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

// selectCheckpointWrite returns the statement that Set runs: the MERGE if the
// user can run it, else a call of the deprecated stored procedure.
func selectCheckpointWrite(ctx context.Context, db *sql.DB, tbl cacheTable, metrics *service.Metrics, log *service.Logger) (string, error) {
	// 1 while this pipeline writes with the deprecated procedure. The removal of
	// the procedure is safe when no pipeline reports 1.
	legacyProc := metrics.NewGauge("oracledb_cdc_checkpoint_legacy_procedure")

	query, err := mergeWrite(ctx, db, tbl)
	// Another error does not tell if the MERGE works. Fail, so that Connect tries again.
	if _, missing := replication.IsMissingPrivilege(err); !missing {
		if err == nil {
			legacyProc.Set(0)
		}
		return query, err
	}

	legacyProc.Set(1)
	log.Warnf("%v. Checkpoints are written with the deprecated stored procedure %s.%s. "+
		"A future version removes this procedure and then fails to start without these privileges. "+
		"Grant them now: GRANT SELECT, INSERT, UPDATE ON %s TO <user>",
		err, tbl.schema, defaultStoredProcName, tbl.String())
	return procedureWrite(ctx, db, tbl)
}

// mergeWrite returns the MERGE that upserts a checkpoint in tbl, with the key
// as :1 and the value as :2. It first probes the MERGE, so it returns an error
// if the user cannot run it.
func mergeWrite(ctx context.Context, db *sql.DB, tbl cacheTable) (string, error) {
	query := fmt.Sprintf(`
		MERGE INTO %s t
		USING (SELECT :1 AS cache_key, :2 AS cache_val FROM dual) s
		ON (t.cache_key = s.cache_key)
		WHEN MATCHED THEN UPDATE SET t.cache_val = s.cache_val
		WHEN NOT MATCHED THEN INSERT (cache_key, cache_val) VALUES (s.cache_key, s.cache_val)`, tbl.String())

	ctx, cancel := context.WithTimeout(ctx, checkpointMergeProbeTimeout)
	defer cancel()
	// go-ora driver handles []byte parameters as RAW type
	if err := replication.ProbeStatement(ctx, db, query, checkpointMergeProbeKey, make([]byte, 8)); err != nil {
		return "", fmt.Errorf("the Connect user cannot write checkpoints to '%s' with MERGE: %w", tbl.String(), err)
	}
	return query, nil
}

// procedureWrite returns the call of the deprecated stored procedure, with the
// key as :1 and the value as :2. It creates the procedure if it does not exist.
func procedureWrite(ctx context.Context, db *sql.DB, tbl cacheTable) (string, error) {
	if err := createUpsertStoredProc(ctx, db, tbl); err != nil {
		return "", fmt.Errorf("creating checkpoint cache write stored procedure: %w", err)
	}
	return fmt.Sprintf("BEGIN %s.%s(:1, :2); END;", tbl.schema, defaultStoredProcName), nil
}

// Get a cache item, we only do this at start up
func (c *checkpointCache) Get(ctx context.Context, key string) ([]byte, error) {
	if c.db == nil {
		return nil, fmt.Errorf("checkpoint cache not initialised for get operation: %w", service.ErrNotConnected)
	}

	var val []byte
	q := "SELECT cache_val FROM %s WHERE cache_key = :1"
	if err := c.db.QueryRowContext(ctx, fmt.Sprintf(q, c.cacheTableName.String()), key).Scan(&val); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, service.ErrKeyNotFound
		}
		return nil, fmt.Errorf("querying checkpoint cache: %w", err)
	}

	// Validate the SCN bytes before returning
	scn, err := replication.SCNFromBytes(val)
	if err != nil {
		return nil, fmt.Errorf("parsing cached SCN bytes: %w", err)
	}
	return scn.Bytes(), nil
}

// Set a cache item, specifying an optional TTL. It is okay for caches to
// ignore the ttl parameter if it isn't possible to implement.
func (c *checkpointCache) Set(ctx context.Context, key string, value []byte, _ *time.Duration) error {
	if c.cacheSetStmt == nil {
		return errors.New("prepared statement for cache set not initialised")
	}
	// go-ora driver handles []byte parameters as RAW type
	if _, err := c.cacheSetStmt.ExecContext(ctx, key, value); err != nil {
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

// migrateCacheTable is a temporary migration function to migrate existing customers' cache. After a while this, along with the test can
// be deleted once we're happy customers are on later versions.
func migrateCacheTable(ctx context.Context, db *sql.DB, tbl cacheTable, cacheKey string, log *service.Logger) error {
	var charLen int
	colQuery := `SELECT CHAR_LENGTH FROM all_tab_columns WHERE owner = :1 AND table_name = :2 AND column_name = 'CACHE_KEY'`
	if err := db.QueryRowContext(ctx, colQuery, strings.ToUpper(tbl.schema), strings.ToUpper(tbl.name)).Scan(&charLen); err != nil {
		return fmt.Errorf("checking cache_key column size: %w", err)
	}

	// Step 1: Widen the cache_key column.
	if charLen < checkpointCacheKeyLimit {
		log.Infof("Checkpoint Migration: Found checkpoint cache table '%s', updating cache_key schema to VARCHAR2(128)", tbl.String())
		alterQuery := fmt.Sprintf(`ALTER TABLE %s MODIFY (cache_key VARCHAR2(128))`, tbl.String())
		if _, err := db.ExecContext(ctx, alterQuery); err != nil {
			return fmt.Errorf("migrating cache_key column to VARCHAR2(128): %w", err)
		}
	}

	// Step 2: Rename any legacy 'max_scn' row to the configured cache key.
	updateQuery := fmt.Sprintf(`UPDATE %s SET cache_key = :1 WHERE cache_key = 'max_scn'`, tbl.String())
	result, err := db.ExecContext(ctx, updateQuery, cacheKey)
	if err != nil {
		return fmt.Errorf("migrating cache key from 'max_scn' to '%s': %w", cacheKey, err)
	}
	if rows, _ := result.RowsAffected(); rows > 0 {
		log.Infof("Checkpoint Migration: Updated cache key from 'max_scn' to '%s' in '%s'", cacheKey, tbl.String())
		log.Info("Checkpoint Migration: Checkpoint cache table migration complete")
	}

	return nil
}

// cacheTableExists is false for a table that the connected user has no privilege on.
func cacheTableExists(ctx context.Context, db *sql.DB, tbl cacheTable) (bool, error) {
	var count int
	checkQuery := `SELECT COUNT(*) FROM all_tables WHERE owner = :1 AND table_name = :2`
	if err := db.QueryRowContext(ctx, checkQuery, strings.ToUpper(tbl.schema), strings.ToUpper(tbl.name)).Scan(&count); err != nil {
		return false, fmt.Errorf("checking if table exists: %w", err)
	}
	return count > 0, nil
}

func createCacheTable(ctx context.Context, db *sql.DB, tbl cacheTable, cacheKey string, log *service.Logger) (bool, error) {
	exists, err := cacheTableExists(ctx, db, tbl)
	if err != nil {
		return false, err
	}

	if exists {
		if err := migrateCacheTable(ctx, db, tbl, cacheKey, log); err != nil {
			return false, fmt.Errorf("applying migration to cache table: %w", err)
		}
		// migration applied
		return false, nil
	}

	// Create table if it doesn't exist
	// cache_val stores binary data as RAW (8 bytes for SCN uint64)
	createQuery := fmt.Sprintf(`
		CREATE TABLE %s (
			cache_key VARCHAR2(128) NOT NULL PRIMARY KEY,
			cache_val RAW(8)
		)`, tbl.String())

	if _, err := db.ExecContext(ctx, createQuery); err != nil {
		// Another pipeline can create the table after our check. Its table has the current layout, so no
		// migration is necessary. Re-check because the name can also belong to a view or to another user's table.
		var oraErr *goora.OracleError
		if errors.As(err, &oraErr) && oraErr.ErrCode == errCodeNameAlreadyUsed {
			exists, checkErr := cacheTableExists(ctx, db, tbl)
			if checkErr != nil {
				return false, fmt.Errorf("creating table: %w (re-check failed: %v)", err, checkErr)
			}
			if exists {
				return false, nil
			}
			return false, fmt.Errorf("creating table: name is used by an object that is not a table visible to this user: %w", err)
		}
		return false, fmt.Errorf("creating table: %w", err)
	}

	return true, nil
}

func createUpsertStoredProc(ctx context.Context, db *sql.DB, cacheTable cacheTable) error {
	// Check if stored proc already exists
	var count int
	q := `SELECT COUNT(*) FROM ALL_PROCEDURES WHERE OWNER = :1 AND OBJECT_NAME = :2 AND OBJECT_TYPE = 'PROCEDURE'`
	if err := db.QueryRowContext(ctx, q, strings.ToUpper(cacheTable.schema), strings.ToUpper(defaultStoredProcName)).Scan(&count); err != nil {
		return fmt.Errorf("checking if stored procedure exists: %w", err)
	}
	if count > 0 {
		return nil
	}

	// Create the upsert procedure
	// Note: go-ora driver handles []byte parameters as RAW type
	storedProcFullName := fmt.Sprintf("%s.%s", cacheTable.schema, defaultStoredProcName)
	tableName := cacheTable.String()

	createQuery := fmt.Sprintf(`
		CREATE PROCEDURE %s (
			p_key IN VARCHAR2,
			p_value IN RAW
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
	return errors.New("function Add not supported for checkpoint cache")
}

// Delete is unused
func (*checkpointCache) Delete(_ context.Context, _ string) error {
	return errors.New("function Delete not supported for checkpoint cache")
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

func validateCacheKey(key string) error {
	if key == "" {
		return errors.New("checkpoint cache key must not be empty")
	}
	// len() gives byte count, which matches Oracle's default byte-semantic VARCHAR2(128) column.
	if len(key) > checkpointCacheKeyLimit {
		return fmt.Errorf("checkpoint cache key must not exceed %d characters", checkpointCacheKeyLimit)
	}
	return nil
}
