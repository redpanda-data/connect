// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package saphana

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"strconv"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/Jeffail/checkpoint"
	gohdb "github.com/SAP/go-hdb/driver"

	"github.com/redpanda-data/benthos/v4/public/schema"
	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/redpanda-data/connect/v4/internal/license"
	"github.com/redpanda-data/connect/v4/internal/sqlutil"
)

const (
	shFieldDSN                    = "dsn"
	shFieldFetchSize              = "fetch_size"
	shFieldSchemaName             = "schema_name"
	shFieldTable                  = "table"
	shFieldMode                   = "mode"
	shFieldQuery                  = "query"
	shFieldIncrementingColumn     = "incrementing_column"
	shFieldIncrementingInitialVal = "incrementing_initial_value"
	shFieldPollInterval           = "poll_interval"

	shFieldTimestampColumn     = "timestamp_column"
	shFieldTimestampInitialVal = "timestamp_initial_value"
	shFieldTimestampDelay      = "timestamp_delay"
	shFieldTimestampClock      = "timestamp_clock"

	shTimestampClockDatabase    = "database"
	shTimestampClockDatabaseUTC = "database_utc"

	shFieldCheckpointCache    = "checkpoint_cache"
	shFieldCheckpointCacheKey = "checkpoint_cache_key"
	shFieldCheckpointLimit    = "checkpoint_limit"

	shFieldNumericMapping   = "numeric_mapping"
	shFieldMaxRetries       = "max_retries"
	shFieldRetryBackoff     = "retry_backoff"
	shNumericMappingNone    = "none"
	shNumericMappingBestFit = "best_fit"

	shModeBulk                  = "bulk"
	shModeIncrementing          = "incrementing"
	shModeQuery                 = "query"
	shModeTimestamp             = "timestamp"
	shModeTimestampIncrementing = "timestamp+incrementing"
)

var sapHANAInputConfigSpec = service.NewConfigSpec().
	Categories("Services").
	Version("4.92.0").
	Summary("Reads rows from a SAP HANA table.").
	Description(`Reads rows from a SAP HANA table. Supports five modes:

- ` + "`bulk`" + `: reads all rows once then the input terminates (use with xref:components:inputs/sequence.adoc[sequence] for periodic re-reads).
- ` + "`incrementing`" + `: polls for rows where ` + "`incrementing_column`" + ` exceeds the last seen value, emitting only net-new rows.
- ` + "`query`" + `: executes a user-supplied SQL statement and emits one message per result row.
- ` + "`timestamp`" + `: polls for rows where ` + "`timestamp_column`" + ` falls within ` + "`(last_hwm, database_now - timestamp_delay]`" + `. The bound is read from the database clock (see ` + "`timestamp_clock`" + `) and the delay absorbs commit lag. The HWM advances to the window bound once the window is fully consumed, so a restart mid-window re-reads that window from its start (rows sharing a timestamp cannot be split, hence no finer checkpoint).
- ` + "`timestamp+incrementing`" + `: like ` + "`timestamp`" + ` but orders by ` + "`(timestamp_column, incrementing_column)`" + ` and resumes after the exact ` + "`(timestamp, incrementing)`" + ` pair of the last delivered row, so rows sharing a timestamp are neither duplicated nor missed and a restart mid-window continues from the last acknowledged batch rather than the window start.

== Metadata

Messages produced in ` + "`bulk`, `incrementing`, `timestamp`, and `timestamp+incrementing`" + ` modes carry the following metadata fields (` + "`query`" + ` mode attaches none):

- ` + "`table_name`" + `: The HANA table name.
- ` + "`database_schema`" + `: The configured ` + "`schema_name`" + `. Only present when ` + "`schema_name`" + ` is set.
- ` + "`schema`" + `: Avro-compatible schema derived from ` + "`SYS.TABLE_COLUMNS`" + `, suitable for use with ` + "`schema_registry_encode`" + `. Column additions are detected automatically without a pipeline restart. Only present when ` + "`schema_name`" + ` is configured.
- ` + "`primary_key_columns`" + `: JSON array of the table's primary-key column names in key order. Only present when ` + "`schema_name`" + ` is configured and the table has a primary key.
`).
	Field(service.NewStringField(shFieldDSN).
		Description("SAP HANA connection DSN in `hdb://user:password@host:port` form.").
		Example("hdb://user:password@host:39017").
		Secret(),
	).
	Field(service.NewIntField(shFieldFetchSize).
		Description("Number of rows requested per FetchNext round-trip. Larger values reduce round-trips on high-latency connections.").
		Default(128).
		Advanced(),
	).
	Field(service.NewStringField(shFieldSchemaName).
		Description("Database schema for the table. When set, an Avro-compatible `schema` metadata field is attached to every message using data from `SYS.TABLE_COLUMNS`.").
		Optional(),
	).
	Field(service.NewStringField(shFieldTable).
		Description("Table to read from. Required when `mode` is `bulk` or `incrementing`.").
		Optional(),
	).
	Field(service.NewStringEnumField(shFieldMode, shModeBulk, shModeIncrementing, shModeQuery, shModeTimestamp, shModeTimestampIncrementing).
		Description("Operation mode.").
		Default(shModeBulk),
	).
	Field(service.NewStringField(shFieldQuery).
		Description("Custom SQL statement to execute. Only used when `mode` is `query`.").
		Optional(),
	).
	Field(service.NewStringField(shFieldIncrementingColumn).
		Description("Column to use as the high-water mark for `incrementing` mode. Must be strictly monotonically increasing with no duplicate values — a BIGINT auto-increment column is ideal. Columns that produce duplicate values (e.g. a plain TIMESTAMP) will cause rows whose value ties span a `fetch_size` boundary to be re-delivered. Use `timestamp+incrementing` mode to handle timestamp columns safely.").
		Optional(),
	).
	Field(service.NewStringField(shFieldIncrementingInitialVal).
		Description("Initial high-water mark value. When empty, all existing rows are emitted on the first run. The value is converted to the `incrementing_column`'s type from the catalog on connect: integers for integer columns, RFC3339 or `YYYY-MM-DD[ HH:MM:SS]` for DATE/TIMESTAMP columns, and the literal string (leading zeros preserved) for character columns. A persisted checkpoint takes precedence over this value.").
		Default(""),
	).
	Field(service.NewDurationField(shFieldPollInterval).
		Description("How long to wait between polls in `incrementing`, `timestamp`, and `timestamp+incrementing` modes.").
		Default("60s").
		Example("10s").
		Example("5m"),
	).
	Field(service.NewStringField(shFieldTimestampColumn).
		Description("Column to use as the high-water mark for `timestamp` and `timestamp+incrementing` modes. Must be a TIMESTAMP or LONGDATE column.").
		Optional(),
	).
	Field(service.NewStringField(shFieldTimestampInitialVal).
		Description("Initial high-water mark in RFC3339 format (e.g. `2024-01-01T00:00:00Z`). When empty, all existing rows are emitted on the first run.").
		Default(""),
	).
	Field(service.NewDurationField(shFieldTimestampDelay).
		Description("Commit-lag buffer for timestamp modes. The upper bound for each poll is the database clock minus `timestamp_delay`, so rows whose timestamp was assigned slightly before a still-uncommitted transaction finished are not missed.").
		Default("5s").
		Example("0s").
		Example("30s"),
	).
	Field(service.NewStringEnumField(shFieldTimestampClock, shTimestampClockDatabase, shTimestampClockDatabaseUTC).
		Description("Which database clock bounds each timestamp-mode poll window. The bound is always read from HANA, never from the connector host, so it shares the clock and timezone convention of the column it is compared against:\n\n- `database`: `CURRENT_TIMESTAMP` (session timezone), matching columns populated by `DEFAULT CURRENT_TIMESTAMP` or `NOW()`.\n- `database_utc`: `CURRENT_UTCTIMESTAMP`, for columns populated with UTC values.").
		Default(shTimestampClockDatabase).
		Advanced(),
	).
	Field(service.NewStringEnumField(shFieldNumericMapping, shNumericMappingNone, shNumericMappingBestFit).
		Description("Controls how DECIMAL/NUMERIC columns are emitted:\n\n- `none`: emit as a canonical decimal string preserving full precision, except integer-typed columns (`DECIMAL(p,0)` with precision <= 18) which are emitted as integers.\n- `best_fit`: additionally emit columns whose precision fits a double (precision <= 15) as floating-point numbers; wider values fall back to canonical decimal strings.").
		Default(shNumericMappingNone),
	).
	Field(service.NewIntField(shFieldMaxRetries).
		Description("Maximum number of times to retry a failed query before returning an error. Set to `0` to disable retries.").
		Default(3).
		Advanced(),
	).
	Field(service.NewDurationField(shFieldRetryBackoff).
		Description("Base delay between query retries. The delay grows linearly with the attempt number (attempt N waits N times this value).").
		Default("1s").
		Advanced(),
	).
	Field(service.NewStringField(shFieldCheckpointCache).
		Description("Name of a cache resource to persist the high-water mark across restarts. When set, the connector resumes from where it left off rather than starting from scratch. Only effective in `incrementing`, `timestamp`, and `timestamp+incrementing` modes. The cache must be declared under `cache_resources`. Choose a durable backend (Redis, PostgreSQL) for production; in-memory caches lose state on restart.").
		Example("redis_cache").
		Optional(),
	).
	Field(service.NewStringField(shFieldCheckpointCacheKey).
		Description("Key used to store the checkpoint in `checkpoint_cache`. Change this when multiple `sap_hana` inputs share the same cache resource to avoid key collisions.").
		Default("sap_hana_hwm").
		Advanced(),
	).
	Field(service.NewIntField(shFieldCheckpointLimit).
		Description("The maximum number of messages that can be in flight (read but not yet acknowledged) at a given time. The high-water mark is only checkpointed once every message before it has been acknowledged, preserving at-least-once delivery even when batches are acknowledged out of order. When `fetch_size` exceeds this limit, batches are delivered one at a time (each waits for the previous batch's acknowledgement), so raise this alongside large `fetch_size` values to keep batches flowing concurrently.").
		Default(1024).
		Advanced(),
	).
	Field(service.NewAutoRetryNacksToggleField())

func init() {
	service.MustRegisterBatchInput("sap_hana", sapHANAInputConfigSpec,
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchInput, error) {
			i, err := newSAPHANAInput(conf, mgr)
			if err != nil {
				return nil, err
			}
			return service.AutoRetryNacksBatchedToggled(conf, i)
		})
}

type sapHANAInput struct {
	dsn             string
	fetchSize       int
	schemaName      string
	tableName       string
	mode            string
	customQuery     string
	incrementingCol string
	incrInitialRaw  string // incrementing_initial_value as configured, before type coercion
	hwm             any
	hwmSafe         any              // last checkpointable HWM: highest value whose tie-group is fully emitted
	peekedRow       *service.Message // row buffered from peek-ahead at fetch_size boundary
	pollInterval    time.Duration

	timestampCol   string
	timestampHWM   time.Time
	lastRowTS      time.Time // timestamp of the most recently scanned row (timestamp+incrementing mid-window checkpoints)
	tsQueryUpper   time.Time
	timestampDelay time.Duration
	timestampClock string

	numericMapping     string
	maxRetries         int
	retryBackoff       time.Duration
	checkpointCache    string
	checkpointCacheKey string
	checkpointLimit    int
	mgr                *service.Resources

	// cpTracker orders checkpoint persistence by delivery order: a batch's HWM
	// snapshot is only persisted once every earlier batch has also resolved.
	cpTracker *checkpoint.Capped[*sapHANACheckpointState]
	// ackMut makes resolve-then-persist atomic so a lower checkpoint can never
	// overwrite a higher one when acks land concurrently.
	ackMut sync.Mutex
	// lastPersisted is the marshalled state most recently written to the
	// cache, used to skip writes that would not change it. Guarded by ackMut.
	lastPersisted []byte

	db      *sql.DB
	rows    *sql.Rows
	dbMut   sync.Mutex
	schemas *schemaCache
	log     *service.Logger

	stopChan chan struct{}
	stopOnce sync.Once

	bulkExhausted bool
	// polledOnce gates the poll_interval wait so the first query of a polling
	// mode runs immediately on startup.
	polledOnce bool

	rowColNames       []string
	rowValues         []any
	rowPtrs           []any
	rowCachedSchema   any
	rowCachedPKCols   []string
	rowCachedColTypes map[string]schema.Common
	rowDriverColTypes map[string]schema.Common // from rows.ColumnTypes(); fallback when the catalog schema is unavailable
	rowSchemaFetched  bool
}

func newSAPHANAInput(conf *service.ParsedConfig, mgr *service.Resources) (*sapHANAInput, error) {
	if err := license.CheckRunningEnterprise(mgr); err != nil {
		return nil, err
	}

	s := &sapHANAInput{
		log:      mgr.Logger(),
		mgr:      mgr,
		stopChan: make(chan struct{}),
	}

	var err error
	if s.dsn, err = conf.FieldString(shFieldDSN); err != nil {
		return nil, err
	}
	if s.fetchSize, err = conf.FieldInt(shFieldFetchSize); err != nil {
		return nil, err
	}
	if s.fetchSize < 1 {
		return nil, fmt.Errorf("field %q must be at least 1", shFieldFetchSize)
	}
	if conf.Contains(shFieldSchemaName) {
		if s.schemaName, err = conf.FieldString(shFieldSchemaName); err != nil {
			return nil, err
		}
	}
	if conf.Contains(shFieldTable) {
		if s.tableName, err = conf.FieldString(shFieldTable); err != nil {
			return nil, err
		}
	}
	if s.mode, err = conf.FieldString(shFieldMode); err != nil {
		return nil, err
	}
	if conf.Contains(shFieldQuery) {
		if s.customQuery, err = conf.FieldString(shFieldQuery); err != nil {
			return nil, err
		}
	}
	if conf.Contains(shFieldIncrementingColumn) {
		if s.incrementingCol, err = conf.FieldString(shFieldIncrementingColumn); err != nil {
			return nil, err
		}
	}
	var hwmInit string
	if hwmInit, err = conf.FieldString(shFieldIncrementingInitialVal); err != nil {
		return nil, err
	}
	if hwmInit != "" {
		// Best-effort guess at the bind type; Connect replaces it with the
		// incrementing column's catalog type once a connection exists.
		s.incrInitialRaw = hwmInit
		s.hwm = parseIncrHWMString(hwmInit)
	}
	if s.pollInterval, err = conf.FieldDuration(shFieldPollInterval); err != nil {
		return nil, err
	}
	if conf.Contains(shFieldTimestampColumn) {
		if s.timestampCol, err = conf.FieldString(shFieldTimestampColumn); err != nil {
			return nil, err
		}
	}
	var tsInitStr string
	if tsInitStr, err = conf.FieldString(shFieldTimestampInitialVal); err != nil {
		return nil, err
	}
	if tsInitStr != "" {
		if s.timestampHWM, err = time.Parse(time.RFC3339, tsInitStr); err != nil {
			return nil, fmt.Errorf("parsing %s: %w", shFieldTimestampInitialVal, err)
		}
	}
	if s.timestampDelay, err = conf.FieldDuration(shFieldTimestampDelay); err != nil {
		return nil, err
	}
	if s.timestampClock, err = conf.FieldString(shFieldTimestampClock); err != nil {
		return nil, err
	}
	if s.numericMapping, err = conf.FieldString(shFieldNumericMapping); err != nil {
		return nil, err
	}
	if s.maxRetries, err = conf.FieldInt(shFieldMaxRetries); err != nil {
		return nil, err
	}
	if s.retryBackoff, err = conf.FieldDuration(shFieldRetryBackoff); err != nil {
		return nil, err
	}
	if conf.Contains(shFieldCheckpointCache) {
		if s.checkpointCache, err = conf.FieldString(shFieldCheckpointCache); err != nil {
			return nil, err
		}
	}
	if s.checkpointCacheKey, err = conf.FieldString(shFieldCheckpointCacheKey); err != nil {
		return nil, err
	}
	if s.checkpointLimit, err = conf.FieldInt(shFieldCheckpointLimit); err != nil {
		return nil, err
	}
	if s.checkpointLimit < 1 {
		return nil, fmt.Errorf("field %q must be at least 1", shFieldCheckpointLimit)
	}
	if s.fetchSize > s.checkpointLimit {
		s.log.Warnf("%s (%d) exceeds %s (%d): batches will be delivered one at a time, each waiting for the previous batch's acknowledgement. Raise %s to allow concurrent batches.",
			shFieldFetchSize, s.fetchSize, shFieldCheckpointLimit, s.checkpointLimit, shFieldCheckpointLimit)
	}
	s.cpTracker = checkpoint.NewCapped[*sapHANACheckpointState](int64(s.checkpointLimit))

	switch s.mode {
	case shModeBulk, shModeIncrementing, shModeTimestamp, shModeTimestampIncrementing:
		if s.tableName == "" {
			return nil, fmt.Errorf("field %q is required when mode is %q", shFieldTable, s.mode)
		}
	case shModeQuery:
		if s.customQuery == "" {
			return nil, fmt.Errorf("field %q is required when mode is %q", shFieldQuery, s.mode)
		}
	}
	if s.mode == shModeIncrementing && s.incrementingCol == "" {
		return nil, fmt.Errorf("field %q is required when mode is %q", shFieldIncrementingColumn, shModeIncrementing)
	}
	if (s.mode == shModeTimestamp || s.mode == shModeTimestampIncrementing) && s.timestampCol == "" {
		return nil, fmt.Errorf("field %q is required when mode is %q", shFieldTimestampColumn, s.mode)
	}
	if s.mode == shModeTimestampIncrementing && s.incrementingCol == "" {
		return nil, fmt.Errorf("field %q is required when mode is %q", shFieldIncrementingColumn, shModeTimestampIncrementing)
	}

	return s, nil
}

func (s *sapHANAInput) Connect(ctx context.Context) error {
	s.dbMut.Lock()
	defer s.dbMut.Unlock()

	if s.db != nil {
		return nil
	}

	connector, connErr := gohdb.NewDSNConnector(s.dsn)
	if connErr != nil {
		return fmt.Errorf("creating SAP HANA connector: %w", connErr)
	}
	connector.SetFetchSize(s.fetchSize)
	db := sql.OpenDB(connector)
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return fmt.Errorf("pinging SAP HANA: %w", err)
	}

	s.db = db
	s.schemas = newSchemaCache(db, s.log, s.numericMapping)

	resumedHWM, err := s.loadCheckpoint(ctx)
	if err != nil {
		_ = db.Close()
		s.db = nil
		return fmt.Errorf("loading checkpoint: %w", err)
	}
	// A persisted checkpoint already carries the HWM with its real type; only
	// a fresh start binds the configured initial value, which must match the
	// column's type or go-hdb rejects the parameter on every poll.
	if !resumedHWM && s.incrInitialRaw != "" && s.incrementingCol != "" {
		if err := s.resolveIncrementingInitialValue(ctx); err != nil {
			_ = db.Close()
			s.db = nil
			return err
		}
	}
	// hwmSafe must start at the loaded checkpoint value so that a partial
	// batch on the first poll never persists nil and regresses progress.
	s.hwmSafe = s.hwm

	s.log.Debug("Connected to SAP HANA.")
	return nil
}

// tableRef returns a properly quoted and escaped table reference.
func (s *sapHANAInput) tableRef() string {
	if s.schemaName != "" {
		return quoteIdentifier(s.schemaName) + "." + quoteIdentifier(s.tableName)
	}
	return quoteIdentifier(s.tableName)
}

// openRows executes the query for the current mode and returns the result set.
func (s *sapHANAInput) openRows(ctx context.Context) (*sql.Rows, error) {
	switch s.mode {
	case shModeBulk:
		q := `SELECT * FROM ` + s.tableRef()
		return s.db.QueryContext(ctx, q)

	case shModeIncrementing:
		inc := quoteIdentifier(s.incrementingCol)
		if s.hwm == nil {
			q := `SELECT * FROM ` + s.tableRef() + ` ORDER BY ` + inc
			return s.db.QueryContext(ctx, q)
		}
		q := `SELECT * FROM ` + s.tableRef() + ` WHERE ` + inc + ` > ? ORDER BY ` + inc
		return s.db.QueryContext(ctx, q, s.hwm)

	case shModeQuery:
		return s.db.QueryContext(ctx, s.customQuery)

	case shModeTimestamp:
		tsc := quoteIdentifier(s.timestampCol)
		upper, err := s.fetchWindowUpperBound(ctx)
		if err != nil {
			return nil, err
		}
		s.tsQueryUpper = upper
		if s.timestampHWM.IsZero() {
			q := `SELECT * FROM ` + s.tableRef() + ` WHERE ` + tsc + ` <= ? ORDER BY ` + tsc
			return s.db.QueryContext(ctx, q, s.tsQueryUpper)
		}
		q := `SELECT * FROM ` + s.tableRef() + ` WHERE ` + tsc + ` > ? AND ` + tsc + ` <= ? ORDER BY ` + tsc
		return s.db.QueryContext(ctx, q, s.timestampHWM, s.tsQueryUpper)

	case shModeTimestampIncrementing:
		tsc := quoteIdentifier(s.timestampCol)
		inc := quoteIdentifier(s.incrementingCol)
		upper, err := s.fetchWindowUpperBound(ctx)
		if err != nil {
			return nil, err
		}
		s.tsQueryUpper = upper
		if s.timestampHWM.IsZero() {
			q := `SELECT * FROM ` + s.tableRef() + ` WHERE ` + tsc + ` <= ? ORDER BY ` + tsc + `, ` + inc
			return s.db.QueryContext(ctx, q, s.tsQueryUpper)
		}
		if s.hwm == nil {
			// timestampHWM advanced but no incrementing value seen yet (e.g. first window was empty).
			// Use pure timestamp comparison to avoid binding nil against a numeric column.
			q := `SELECT * FROM ` + s.tableRef() +
				` WHERE ` + tsc + ` > ? AND ` + tsc + ` <= ?` +
				` ORDER BY ` + tsc + `, ` + inc
			return s.db.QueryContext(ctx, q, s.timestampHWM, s.tsQueryUpper)
		}
		q := `SELECT * FROM ` + s.tableRef() +
			` WHERE (` + tsc + ` > ? OR (` + tsc + ` = ? AND ` + inc + ` > ?))` +
			` AND ` + tsc + ` <= ?` +
			` ORDER BY ` + tsc + `, ` + inc
		return s.db.QueryContext(ctx, q, s.timestampHWM, s.timestampHWM, s.hwm, s.tsQueryUpper)

	default:
		return nil, fmt.Errorf("unknown mode %q", s.mode)
	}
}

// hanaClockQuery and hanaUTCClockQuery return the database's current time
// shifted by a (negative) number of seconds. The bound must come from the
// database rather than the connector host: a TIMESTAMP column has no
// timezone, so only the clock that populated it can be compared against it,
// and an hours-scale host/database offset would otherwise skip or delay rows
// forever.
const (
	hanaClockQuery    = `SELECT ADD_SECONDS(CURRENT_TIMESTAMP, ?) FROM DUMMY`
	hanaUTCClockQuery = `SELECT ADD_SECONDS(CURRENT_UTCTIMESTAMP, ?) FROM DUMMY`
)

// fetchWindowUpperBound reads the timestamp-mode poll window's upper bound
// (database now minus timestamp_delay) from the configured database clock.
func (s *sapHANAInput) fetchWindowUpperBound(ctx context.Context) (time.Time, error) {
	q := hanaClockQuery
	if s.timestampClock == shTimestampClockDatabaseUTC {
		q = hanaUTCClockQuery
	}
	var upper time.Time
	if err := s.db.QueryRowContext(ctx, q, -s.timestampDelay.Seconds()).Scan(&upper); err != nil {
		return time.Time{}, fmt.Errorf("reading database clock for the poll window: %w", err)
	}
	return upper, nil
}

// openRowsWithRetry calls openRows, retrying up to s.maxRetries times on
// failure with linear backoff. The lock is released during each backoff sleep
// so that Close() can proceed.
func (s *sapHANAInput) openRowsWithRetry(ctx context.Context) (*sql.Rows, error) {
	var err error
	for attempt := 0; attempt <= s.maxRetries; attempt++ {
		if attempt > 0 {
			s.log.Warnf("Query failed (attempt %d/%d), retrying: %v", attempt, s.maxRetries, err)
			s.dbMut.Unlock()
			select {
			case <-ctx.Done():
				s.dbMut.Lock()
				return nil, ctx.Err()
			case <-s.stopChan:
				s.dbMut.Lock()
				return nil, service.ErrEndOfInput
			case <-time.After(time.Duration(attempt) * s.retryBackoff):
			}
			s.dbMut.Lock()
			if s.db == nil {
				return nil, service.ErrNotConnected
			}
		}
		var rows *sql.Rows
		if rows, err = s.openRows(ctx); err == nil {
			return rows, nil
		}
	}
	return nil, err
}

func (s *sapHANAInput) resetCursorCache() {
	s.rowColNames = nil
	s.rowValues = nil
	s.rowPtrs = nil
	s.rowCachedSchema = nil
	s.rowCachedPKCols = nil
	s.rowCachedColTypes = nil
	s.rowDriverColTypes = nil
	s.rowSchemaFetched = false
	s.peekedRow = nil
}

// discardCursor closes the active cursor after an error discarded rows that
// were scanned but never delivered, rewinding the in-memory HWM to the last
// safe value so the next poll re-reads those rows instead of skipping past
// them (scanRow advances s.hwm as rows are scanned, delivered or not).
func (s *sapHANAInput) discardCursor() {
	if s.rows != nil {
		_ = s.rows.Close()
		s.rows = nil
	}
	s.resetCursorCache()
	if s.mode == shModeIncrementing || s.mode == shModeTimestampIncrementing {
		s.hwm = s.hwmSafe
	}
}

func (s *sapHANAInput) ReadBatch(ctx context.Context) (service.MessageBatch, service.AckFunc, error) {
	s.dbMut.Lock()
	defer s.dbMut.Unlock()

	if s.db == nil {
		return nil, nil, service.ErrNotConnected
	}

	if s.bulkExhausted {
		return nil, nil, service.ErrEndOfInput
	}

	for {
		if s.rows == nil {
			switch s.mode {
			case shModeBulk, shModeQuery:
				// Pre-warm schema cache before opening rows so the schema
				// query doesn't compete with the main query for a HANA connection.
				if s.schemaName != "" && s.mode != shModeQuery {
					_, _ = s.schemas.schemaForEvent(ctx, s.schemaName, s.tableName, nil)
				}
				rows, err := s.openRowsWithRetry(ctx) //nolint:rowserrcheck // rows.Err is checked after iteration via s.rows
				if err != nil {
					return nil, nil, fmt.Errorf("executing query: %w", err)
				}
				s.rows = rows
				s.resetCursorCache()

			case shModeIncrementing, shModeTimestamp, shModeTimestampIncrementing:
				// The wait applies between polls, not before the first one: a
				// fresh pipeline should emit waiting rows immediately instead
				// of sitting silent for a full poll_interval.
				if s.polledOnce {
					// Release the lock while waiting so Close() can proceed.
					s.dbMut.Unlock()
					select {
					case <-ctx.Done():
						s.dbMut.Lock()
						return nil, nil, ctx.Err()
					case <-s.stopChan:
						s.dbMut.Lock()
						return nil, nil, service.ErrEndOfInput
					case <-time.After(s.pollInterval):
					}
					s.dbMut.Lock()
					if s.db == nil {
						return nil, nil, service.ErrNotConnected
					}
				}
				s.polledOnce = true
				// Pre-warm schema cache before opening rows (same reason as bulk mode).
				if s.schemaName != "" {
					_, _ = s.schemas.schemaForEvent(ctx, s.schemaName, s.tableName, nil)
				}
				rows, err := s.openRowsWithRetry(ctx) //nolint:rowserrcheck // rows.Err is checked after iteration via s.rows
				if err != nil {
					return nil, nil, fmt.Errorf("executing query: %w", err)
				}
				s.rows = rows
				s.resetCursorCache()
			}
		}

		batch := make(service.MessageBatch, 0, s.fetchSize)
		// Emit the row buffered by the previous batch's peek-ahead before
		// continuing the cursor scan. s.hwm was already updated when peeked.
		if s.peekedRow != nil {
			batch = append(batch, s.peekedRow)
			s.peekedRow = nil
		}
		for s.rows.Next() {
			msg, err := s.scanRow(ctx, s.rows)
			if err != nil {
				s.discardCursor()
				return nil, nil, err
			}
			batch = append(batch, msg)
			if len(batch) >= s.fetchSize {
				if s.mode == shModeIncrementing {
					// Peek ahead to determine whether the current HWM group is
					// complete before committing the checkpoint.  If the next row
					// carries the same incrementing value we are mid-group and
					// must not advance hwmSafe — doing so would skip the remaining
					// tied rows on the next poll.
					hwmAtPeek := s.hwm
					if s.rows.Next() {
						peekedMsg, pErr := s.scanRow(ctx, s.rows)
						if pErr != nil {
							s.discardCursor()
							return nil, nil, pErr
						}
						s.peekedRow = peekedMsg
						if s.hwm != hwmAtPeek {
							// Group boundary crossed: previous value is fully emitted.
							s.hwmSafe = hwmAtPeek
						}
						// else: still mid-group; hwmSafe stays at last complete group.
					} else {
						// Cursor exhausted by the peek: current HWM is safe.
						if rErr := s.rows.Err(); rErr != nil {
							s.discardCursor()
							return nil, nil, fmt.Errorf("iterating rows: %w", rErr)
						}
						_ = s.rows.Close()
						s.rows = nil
						s.resetCursorCache()
						s.hwmSafe = s.hwm
					}
					return s.deliverBatch(ctx, batch, s.hwmSafe)
				}
				if s.mode == shModeTimestampIncrementing {
					// Every scanned row is now handed to the framework (which
					// auto-replays nacks), so the cursor may safely resume from
					// the current HWM after an error.
					s.hwmSafe = s.hwm
					// Mid-window, checkpoint the last delivered row's
					// (timestamp, incrementing) pair: the tie-break predicate
					// resumes exactly after it, instead of re-reading the whole
					// window from its start on restart.
					if !s.lastRowTS.IsZero() {
						return s.deliverBatchAt(ctx, batch, s.hwm, s.lastRowTS)
					}
				}
				return s.deliverBatch(ctx, batch, s.hwm)
			}
		}
		if err := s.rows.Err(); err != nil {
			s.discardCursor()
			return nil, nil, fmt.Errorf("iterating rows: %w", err)
		}
		_ = s.rows.Close()
		s.rows = nil
		s.resetCursorCache()

		if s.mode == shModeTimestamp || s.mode == shModeTimestampIncrementing {
			s.timestampHWM = s.tsQueryUpper
		}
		if s.mode == shModeIncrementing || s.mode == shModeTimestampIncrementing {
			// Cursor fully consumed: every row seen, so current HWM is safe.
			s.hwmSafe = s.hwm
		}
		if s.mode == shModeBulk || s.mode == shModeQuery {
			s.bulkExhausted = true
		}

		if len(batch) > 0 {
			hwmSnap := s.hwm
			if s.mode == shModeIncrementing {
				hwmSnap = s.hwmSafe
			}
			return s.deliverBatch(ctx, batch, hwmSnap)
		}

		// Empty poll: route the persist through the tracker so it stays
		// ordered behind batches still awaiting acks.
		hwmSnap := s.hwm
		if s.mode == shModeIncrementing || s.mode == shModeTimestampIncrementing {
			hwmSnap = s.hwmSafe
		}
		ackFn, err := s.trackBatch(ctx, 0, hwmSnap, s.timestampHWM)
		if err != nil {
			return nil, nil, err
		}
		_ = ackFn(ctx, nil)

		if s.bulkExhausted {
			return nil, nil, service.ErrEndOfInput
		}
	}
}

// parseIncrHWMString converts the string value of incrementing_initial_value
// to the most specific numeric type so that the first poll binds the correct
// wire type instead of a VARCHAR parameter that some HANA versions reject.
// Mirrors the type ladder used by loadCheckpoint.
func parseIncrHWMString(s string) any {
	if i, err := strconv.ParseInt(s, 10, 64); err == nil {
		return i
	}
	if f, err := strconv.ParseFloat(s, 64); err == nil {
		return f
	}
	return s
}

// lobScanner is the shape go-hdb gives a LOB column (CLOB, NCLOB, BLOB, TEXT)
// scanned into *any: the content is only reachable by draining it through
// Scan(io.Writer). Passing such a value to encoding/json yields "{}".
type lobScanner interface {
	Scan(wr io.Writer) error
}

// normalizeHANAValue converts go-hdb-specific types to JSON-friendly Go types.
// NVARCHAR/VARCHAR arrive as []byte off the wire; DECIMAL as gohdb.Decimal
// (big.Rat alias); LOBs as a lob scanner that must be drained while the
// cursor is still open. colType carries schema metadata for the column (nil
// when schema is unavailable). numericMapping controls how DECIMAL/NUMERIC
// values are emitted.
func normalizeHANAValue(v any, colType *schema.Common, numericMapping string) (any, error) {
	switch val := v.(type) {
	case []byte:
		return normalizeBytes(val, colType), nil
	case lobScanner:
		var b []byte
		if err := gohdb.ScanLobBytes(val, &b); err != nil {
			return nil, fmt.Errorf("reading LOB column: %w", err)
		}
		return normalizeBytes(b, colType), nil
	case gohdb.Decimal:
		return normalizeDecimal((*big.Rat)(&val), colType, numericMapping), nil
	case *gohdb.Decimal:
		if val == nil {
			return nil, nil
		}
		return normalizeDecimal((*big.Rat)(val), colType, numericMapping), nil
	case *big.Rat:
		if val == nil {
			return nil, nil
		}
		return normalizeDecimal(val, colType, numericMapping), nil
	}
	return v, nil
}

// normalizeBytes decides whether raw column bytes are text or binary. go-hdb
// returns text columns as []byte, so those convert to string; binary columns
// must stay []byte so JSON base64-encodes them losslessly — an invalid-UTF-8
// string would be mangled into U+FFFD replacement characters by encoding/json.
func normalizeBytes(val []byte, colType *schema.Common) any {
	if colType != nil {
		if colType.Type == schema.ByteArray {
			return val
		}
		return string(val)
	}
	if utf8.Valid(val) {
		return string(val)
	}
	return val
}

// normalizeDecimal converts a big.Rat decimal to the representation selected
// by numericMapping, guided by the column's schema type when available.
func normalizeDecimal(r *big.Rat, colType *schema.Common, numericMapping string) any {
	// Determine the canonical form from schema type when available.
	if colType != nil {
		switch colType.Type {
		case schema.Int64:
			// DECIMAL(p,0) that fits in int64 — return the integer directly.
			if r.IsInt() && r.Num().IsInt64() {
				return r.Num().Int64()
			}
		case schema.Float64:
			// best_fit mapped this column to a double at the schema layer.
			f, _ := r.Float64()
			return f
		case schema.Decimal:
			if colType.Logical != nil && colType.Logical.Decimal != nil {
				p := colType.Logical.Decimal.Precision
				s := colType.Logical.Decimal.Scale
				// Pass the exact value, not one pre-rounded to the cached
				// scale: the schema cache is addition-only, so after an online
				// ALTER widens the scale the helper's over-scale rejection is
				// what keeps values from being silently truncated (falling
				// through to the exact BigDecimal path below).
				if out, err := sqlutil.CanonicaliseDecimal(ratToNaturalDecimalString(r), p, s); err == nil {
					return out
				}
			}
		}
	}

	// Without schema guidance best_fit still maps values that fit: integers
	// to int64, values within float64's safe digit range to float64.
	if numericMapping == shNumericMappingBestFit {
		if r.IsInt() && r.Num().IsInt64() {
			return r.Num().Int64()
		}
		if text := ratToNaturalDecimalString(r); decimalFitsFloat64(text) {
			if f, err := strconv.ParseFloat(text, 64); err == nil {
				return f
			}
		}
	}

	// BigDecimal fallback: recover the natural scale from the denominator
	// (HANA DECIMAL denominators are always powers of 10) then canonicalise.
	if out, err := sqlutil.CanonicaliseBigDecimal(ratToNaturalDecimalString(r)); err == nil {
		return out
	}
	f, _ := r.Float64()
	return f
}

// decimalFitsFloat64 reports whether a decimal string's significant digits fit
// within float64's lossless range.
func decimalFitsFloat64(text string) bool {
	digits := 0
	seenNonZero := false
	for _, c := range text {
		if c < '0' || c > '9' {
			continue
		}
		if c == '0' && !seenNonZero {
			continue
		}
		seenNonZero = true
		digits++
	}
	return digits <= float64MaxSafeDigits
}

// ratToNaturalDecimalString converts a *big.Rat to a decimal string using the
// minimal number of fractional digits that exactly represent the value.
// big.Rat keeps fractions reduced, so a terminating decimal's denominator is
// 2^a·5^b rather than a power of 10 (0.5 is 1/2, 12.75 is 51/4); the exact
// scale is max(a, b). Any other prime factor means a non-terminating
// expansion, which HANA DECIMAL cannot produce, so that case falls back to a
// fixed 38 digits (HANA's maximum precision).
func ratToNaturalDecimalString(r *big.Rat) string {
	denom := new(big.Int).Set(r.Denom())
	twos := stripFactor(denom, 2)
	fives := stripFactor(denom, 5)
	if denom.Cmp(big.NewInt(1)) != 0 {
		return r.FloatString(38)
	}
	return r.FloatString(max(twos, fives))
}

// stripFactor divides n by p while divisible, returning the multiplicity.
func stripFactor(n *big.Int, p int64) int {
	pBig := big.NewInt(p)
	count := 0
	for {
		q, rem := new(big.Int).DivMod(n, pBig, new(big.Int))
		if rem.Sign() != 0 || n.Sign() == 0 {
			return count
		}
		n.Set(q)
		count++
	}
}

// scanRow reads the current row into a message and attaches metadata.
func (s *sapHANAInput) scanRow(ctx context.Context, rows *sql.Rows) (*service.Message, error) {
	if s.rowColNames == nil {
		colNames, err := rows.Columns()
		if err != nil {
			return nil, fmt.Errorf("getting column names: %w", err)
		}
		s.rowColNames = colNames
		s.rowValues = make([]any, len(colNames))
		s.rowPtrs = make([]any, len(colNames))
		for i := range s.rowValues {
			s.rowPtrs[i] = &s.rowValues[i]
		}
		if s.mode != shModeQuery {
			sr, _ := s.schemas.schemaForEvent(ctx, s.schemaName, s.tableName, colNames)
			if sr != nil {
				s.rowCachedSchema = sr.Val
				s.rowCachedPKCols = sr.PKCols
				s.rowCachedColTypes = sr.ColTypes
			}
			s.rowSchemaFetched = true
		}
		// The driver's result metadata is the fallback when catalog schema
		// metadata is unavailable (query mode, or schema_name unset), so text
		// vs binary and integer vs decimal are decided once per column, not
		// re-guessed from each row's bytes.
		colTypes, err := rows.ColumnTypes()
		if err != nil {
			return nil, fmt.Errorf("getting column types: %w", err)
		}
		s.rowDriverColTypes = driverColumnTypes(colTypes, s.numericMapping)
	}

	if err := rows.Scan(s.rowPtrs...); err != nil {
		return nil, fmt.Errorf("scanning columns: %w", err)
	}

	rowMap := make(map[string]any, len(s.rowColNames))
	for i, name := range s.rowColNames {
		var ct *schema.Common
		if c, ok := s.rowCachedColTypes[name]; ok {
			ct = &c
		} else if c, ok := s.rowDriverColTypes[name]; ok {
			ct = &c
		}
		v, err := normalizeHANAValue(s.rowValues[i], ct, s.numericMapping)
		if err != nil {
			return nil, fmt.Errorf("normalising column %q: %w", name, err)
		}
		rowMap[name] = v
	}

	if (s.mode == shModeIncrementing || s.mode == shModeTimestampIncrementing) && s.incrementingCol != "" {
		if v, ok := rowMap[s.incrementingCol]; ok && v != nil {
			s.hwm = v
		}
	}
	if s.mode == shModeTimestampIncrementing {
		if ts, ok := rowMap[s.timestampCol].(time.Time); ok {
			s.lastRowTS = ts
		}
	}

	b, err := json.Marshal(rowMap)
	if err != nil {
		return nil, fmt.Errorf("marshalling row: %w", err)
	}

	msg := service.NewMessage(b)

	if s.rowSchemaFetched {
		if s.rowCachedSchema != nil {
			// The tree is owned by the schema cache and shared by every
			// message; immutable storage hands downstream mutators a copy.
			msg.MetaSetImmut("schema", service.ImmutableAny{V: s.rowCachedSchema})
		}
		if len(s.rowCachedPKCols) > 0 {
			if pkJSON, merr := json.Marshal(s.rowCachedPKCols); merr == nil {
				msg.MetaSetMut("primary_key_columns", string(pkJSON))
			}
		}
		if s.schemaName != "" {
			msg.MetaSetMut("database_schema", s.schemaName)
		}
		msg.MetaSetMut("table_name", s.tableName)
	}

	return msg, nil
}

// sapHANACheckpointState is the JSON shape persisted to the cache.
// Typed pointer fields preserve the original Go type so bind parameters
// round-trip correctly without implicit string casts.
type sapHANACheckpointState struct {
	TimestampHWM *time.Time `json:"ts_hwm,omitempty"`
	IncrHWMStr   *string    `json:"incr_hwm_str,omitempty"`
	IncrHWMInt   *int64     `json:"incr_hwm_int,omitempty"`
	IncrHWMFloat *float64   `json:"incr_hwm_float,omitempty"`
	IncrHWMTime  *time.Time `json:"incr_hwm_time,omitempty"`
}

// loadCheckpoint restores persisted HWM state from the cache. It reports
// whether an incrementing HWM was restored, since that value supersedes the
// configured initial value.
func (s *sapHANAInput) loadCheckpoint(ctx context.Context) (bool, error) {
	if !s.checkpointingEnabled() {
		return false, nil
	}
	var (
		raw    []byte
		getErr error
	)
	if err := s.mgr.AccessCache(ctx, s.checkpointCache, func(c service.Cache) {
		raw, getErr = c.Get(ctx, s.checkpointCacheKey)
	}); err != nil {
		return false, fmt.Errorf("accessing checkpoint cache %q: %w", s.checkpointCache, err)
	}
	if errors.Is(getErr, service.ErrKeyNotFound) {
		return false, nil
	}
	if getErr != nil {
		return false, fmt.Errorf("reading checkpoint key %q: %w", s.checkpointCacheKey, getErr)
	}

	var cp sapHANACheckpointState
	if err := json.Unmarshal(raw, &cp); err != nil {
		return false, fmt.Errorf("parsing checkpoint: %w", err)
	}
	if cp.TimestampHWM != nil {
		s.timestampHWM = *cp.TimestampHWM
	}
	resumedHWM := true
	switch {
	case cp.IncrHWMStr != nil:
		s.hwm = *cp.IncrHWMStr
	case cp.IncrHWMInt != nil:
		s.hwm = *cp.IncrHWMInt
	case cp.IncrHWMFloat != nil:
		s.hwm = *cp.IncrHWMFloat
	case cp.IncrHWMTime != nil:
		s.hwm = *cp.IncrHWMTime
	default:
		resumedHWM = false
	}
	s.log.Debugf("Loaded checkpoint: ts_hwm=%v incr_hwm=%v", s.timestampHWM, s.hwm)
	return resumedHWM, nil
}

// resolveIncrementingInitialValue coerces the configured
// incrementing_initial_value to the incrementing column's catalog type. YAML
// only gives us a string, and go-hdb converts bind parameters client-side
// against the prepared statement's metadata: an int64 against an NVARCHAR
// key or a string against a TIMESTAMP is rejected on every poll, so the
// input would never progress. If the catalog is unreadable the constructor's
// heuristic guess is kept and the situation is logged.
func (s *sapHANAInput) resolveIncrementingInitialValue(ctx context.Context) error {
	dataType, err := fetchHANAColumnType(ctx, s.db, s.schemaName, s.tableName, s.incrementingCol)
	if err != nil {
		s.log.Warnf("Could not determine the type of %s column %q from SYS.TABLE_COLUMNS, binding %s as %T: %v",
			shFieldIncrementingColumn, s.incrementingCol, shFieldIncrementingInitialVal, s.hwm, err)
		return nil
	}
	v, err := coerceIncrementingValue(s.incrInitialRaw, dataType)
	if err != nil {
		return fmt.Errorf("%s %q does not match %s %q of type %s: %w",
			shFieldIncrementingInitialVal, s.incrInitialRaw, shFieldIncrementingColumn, s.incrementingCol, dataType, err)
	}
	s.hwm = v
	return nil
}

// incrementingTimeLayouts are the accepted spellings of a DATE/TIMESTAMP
// initial value, tried in order.
var incrementingTimeLayouts = []string{
	time.RFC3339Nano,
	"2006-01-02 15:04:05.999999999",
	"2006-01-02 15:04:05",
	"2006-01-02",
}

// coerceIncrementingValue converts the configured initial value to the Go
// type go-hdb expects for a column of the given HANA data type.
func coerceIncrementingValue(raw, dataType string) (any, error) {
	switch dataType {
	case "TINYINT", "SMALLINT", "INT", "INTEGER", "BIGINT":
		i, err := strconv.ParseInt(raw, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("expected an integer: %w", err)
		}
		return i, nil
	case "DECIMAL", "NUMERIC", "SMALLDECIMAL", "REAL", "FLOAT", "DOUBLE":
		f, err := strconv.ParseFloat(raw, 64)
		if err != nil {
			return nil, fmt.Errorf("expected a number: %w", err)
		}
		return f, nil
	case "DATE", "TIME", "TIMESTAMP", "SECONDDATE":
		for _, layout := range incrementingTimeLayouts {
			if t, err := time.Parse(layout, raw); err == nil {
				return t.UTC(), nil
			}
		}
		return nil, errors.New("expected an RFC3339 or 'YYYY-MM-DD[ HH:MM:SS]' timestamp")
	default:
		// Character types (VARCHAR, NVARCHAR, ALPHANUM, ...) bind as-is,
		// preserving leading zeros and other formatting.
		return raw, nil
	}
}

// checkpointSnapshot captures HWM values as the JSON state persisted to the
// cache. Typed pointer fields preserve the original Go type so bind parameters
// round-trip correctly without implicit string casts.
func checkpointSnapshot(hwm any, tsHWM time.Time) *sapHANACheckpointState {
	cp := &sapHANACheckpointState{}
	if !tsHWM.IsZero() {
		cp.TimestampHWM = &tsHWM
	}
	switch v := hwm.(type) {
	case string:
		cp.IncrHWMStr = &v
	case int64:
		cp.IncrHWMInt = &v
	case float64:
		cp.IncrHWMFloat = &v
	case time.Time:
		cp.IncrHWMTime = &v
	}
	return cp
}

// deliverBatch registers the batch with the ack-order tracker and returns it
// alongside the AckFunc that resolves its checkpoint slot.
func (s *sapHANAInput) deliverBatch(ctx context.Context, batch service.MessageBatch, hwm any) (service.MessageBatch, service.AckFunc, error) {
	return s.deliverBatchAt(ctx, batch, hwm, s.timestampHWM)
}

// deliverBatchAt is deliverBatch with an explicit timestamp HWM snapshot.
func (s *sapHANAInput) deliverBatchAt(ctx context.Context, batch service.MessageBatch, hwm any, tsHWM time.Time) (service.MessageBatch, service.AckFunc, error) {
	ackFn, err := s.trackBatch(ctx, len(batch), hwm, tsHWM)
	if err != nil {
		return nil, nil, err
	}
	return batch, ackFn, nil
}

// trackBatch registers a batch's HWM snapshot with the ack-order tracker and
// returns the AckFunc that resolves its slot. The snapshot is only persisted
// once every earlier batch has also resolved, so out-of-order acks can never
// checkpoint past rows still in flight. Nacks resolve like acks: they are
// replayed by auto_replay_nacks (the default), and disabling that is a
// documented opt-in to DROP rejected messages, so the checkpoint must advance
// past them rather than pin the tracker (which would block at
// checkpoint_limit and stall the input permanently).
func (s *sapHANAInput) trackBatch(ctx context.Context, batchLen int, hwm any, tsHWM time.Time) (service.AckFunc, error) {
	resolve, err := s.cpTracker.Track(ctx, checkpointSnapshot(hwm, tsHWM), int64(batchLen))
	if err != nil {
		return nil, fmt.Errorf("tracking batch for checkpointing: %w", err)
	}
	return func(ctx context.Context, ackErr error) error {
		if ackErr != nil {
			s.log.Warnf("Advancing the checkpoint past a batch rejected downstream (auto_replay_nacks is disabled, so the rejected messages are dropped by contract): %v", ackErr)
		}
		// Resolve and persist under one lock so a lower checkpoint can never
		// overwrite a higher one when acks land concurrently.
		s.ackMut.Lock()
		defer s.ackMut.Unlock()
		cp := resolve()
		if cp == nil || *cp == nil {
			return nil
		}
		if saveErr := s.persistCheckpoint(ctx, *cp); saveErr != nil {
			s.log.Warnf("Failed to save checkpoint: %v", saveErr)
		}
		return nil
	}, nil
}

// persistCheckpoint writes the checkpoint state to the configured cache.
func (s *sapHANAInput) persistCheckpoint(ctx context.Context, cp *sapHANACheckpointState) error {
	if !s.checkpointingEnabled() {
		return nil
	}
	b, err := json.Marshal(cp)
	if err != nil {
		return fmt.Errorf("marshalling checkpoint: %w", err)
	}
	// Out-of-order acks behind a pending batch and idle empty polls resolve
	// to the same highest checkpoint again and again; only pay for a cache
	// write when the persisted state actually changes. Callers hold ackMut.
	if bytes.Equal(b, s.lastPersisted) {
		return nil
	}
	var setErr error
	if err := s.mgr.AccessCache(ctx, s.checkpointCache, func(c service.Cache) {
		setErr = c.Set(ctx, s.checkpointCacheKey, b, nil)
	}); err != nil {
		return fmt.Errorf("accessing checkpoint cache %q: %w", s.checkpointCache, err)
	}
	if setErr != nil {
		return fmt.Errorf("writing checkpoint key %q: %w", s.checkpointCacheKey, setErr)
	}
	s.lastPersisted = b
	return nil
}

// checkpointingEnabled reports whether HWM state is loaded from and persisted
// to the cache: only when a cache is configured and the mode has an HWM to
// track. bulk and query modes have none, and writing an empty state from them
// would clobber a polling input sharing the same cache key.
func (s *sapHANAInput) checkpointingEnabled() bool {
	if s.checkpointCache == "" {
		return false
	}
	return s.mode != shModeBulk && s.mode != shModeQuery
}

func (s *sapHANAInput) Close(_ context.Context) error {
	s.stopOnce.Do(func() { close(s.stopChan) })

	s.dbMut.Lock()
	defer s.dbMut.Unlock()

	if s.rows != nil {
		_ = s.rows.Close()
		s.rows = nil
	}
	if s.db != nil {
		err := s.db.Close()
		s.db = nil
		return err
	}
	return nil
}
