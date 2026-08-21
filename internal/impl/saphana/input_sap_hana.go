// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package saphana

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"strconv"
	"sync"
	"time"

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

	shFieldTimestampColumn    = "timestamp_column"
	shFieldTimestampInitialVal = "timestamp_initial_value"
	shFieldTimestampDelay     = "timestamp_delay"

	shFieldCheckpointCache    = "checkpoint_cache"
	shFieldCheckpointCacheKey = "checkpoint_cache_key"

	shFieldNumericMapping            = "numeric_mapping"
	shFieldMaxRetries                = "max_retries"
	shNumericMappingNone    = "none"
	shNumericMappingBestFit = "best_fit"

	shModeBulk                 = "bulk"
	shModeIncrementing          = "incrementing"
	shModeQuery                = "query"
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
- ` + "`timestamp`" + `: polls for rows where ` + "`timestamp_column`" + ` falls within ` + "`(last_hwm, NOW()-timestamp_delay]`" + `, advancing the HWM after each batch. The delay absorbs DB clock skew.
- ` + "`timestamp+incrementing`" + `: like ` + "`timestamp`" + ` but breaks ties within the same timestamp using ` + "`incrementing_column`" + `, preventing duplicate or missed rows when multiple rows share an identical timestamp.

== Metadata

Every message produced by this input carries the following metadata fields:

- ` + "`sap_hana_schema`" + `: The HANA schema name.
- ` + "`sap_hana_table`" + `: The HANA table name.
- ` + "`schema`" + `: Avro-compatible schema derived from ` + "`SYS.TABLE_COLUMNS`" + `, suitable for use with ` + "`schema_registry_encode`" + `. Column additions are detected automatically without a pipeline restart. Only present when ` + "`schema_name`" + ` is configured.
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
		Description("Initial high-water mark value. When empty, all existing rows are emitted on the first run.").
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
		Description("Clock-skew buffer for timestamp modes. The upper bound for each poll is `NOW()-timestamp_delay`, ensuring recently inserted rows are not missed due to clock differences.").
		Default("5s").
		Example("0s").
		Example("30s"),
	).
	Field(service.NewStringEnumField(shFieldNumericMapping, shNumericMappingNone, shNumericMappingBestFit).
		Description("Controls how DECIMAL/NUMERIC columns are emitted:\n\n- `none`: always emit as a canonical decimal string, preserving full precision.\n- `best_fit`: emit as `int64` when scale is 0 and the value fits; otherwise emit as a canonical decimal string.").
		Default(shNumericMappingNone),
	).
	Field(service.NewIntField(shFieldMaxRetries).
		Description("Maximum number of times to retry a failed query before returning an error. Set to `0` to disable retries.").
		Default(3).
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
	hwm             any
	hwmSafe         any // last checkpointable HWM: highest value whose tie-group is fully emitted
	peekedRow       *service.Message // row buffered from peek-ahead at fetch_size boundary
	pollInterval    time.Duration

	timestampCol   string
	timestampHWM   time.Time
	tsQueryUpper   time.Time
	timestampDelay time.Duration

	numericMapping     string
	maxRetries         int
	checkpointCache    string
	checkpointCacheKey string
	mgr                *service.Resources

	db      *sql.DB
	rows    *sql.Rows
	dbMut   sync.Mutex
	schemas *schemaCache
	log     *service.Logger

	stopChan chan struct{}
	stopOnce sync.Once

	bulkExhausted bool

	rowColNames      []string
	rowValues        []any
	rowPtrs          []any
	rowCachedSchema  any
	rowCachedPKCols  []string
	rowCachedColTypes map[string]schema.Common
	rowSchemaFetched bool
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
	if s.numericMapping, err = conf.FieldString(shFieldNumericMapping); err != nil {
		return nil, err
	}
	if s.maxRetries, err = conf.FieldInt(shFieldMaxRetries); err != nil {
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
	s.schemas = newSchemaCache(db, s.log)

	if err := s.loadCheckpoint(ctx); err != nil {
		_ = db.Close()
		s.db = nil
		return fmt.Errorf("loading checkpoint: %w", err)
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
		s.tsQueryUpper = time.Now().Add(-s.timestampDelay)
		if s.timestampHWM.IsZero() {
			q := `SELECT * FROM ` + s.tableRef() + ` WHERE ` + tsc + ` <= ? ORDER BY ` + tsc
			return s.db.QueryContext(ctx, q, s.tsQueryUpper)
		}
		q := `SELECT * FROM ` + s.tableRef() + ` WHERE ` + tsc + ` > ? AND ` + tsc + ` <= ? ORDER BY ` + tsc
		return s.db.QueryContext(ctx, q, s.timestampHWM, s.tsQueryUpper)

	case shModeTimestampIncrementing:
		tsc := quoteIdentifier(s.timestampCol)
		inc := quoteIdentifier(s.incrementingCol)
		s.tsQueryUpper = time.Now().Add(-s.timestampDelay)
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
			case <-time.After(time.Duration(attempt) * time.Second):
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
	s.rowSchemaFetched = false
	s.peekedRow = nil
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
				rows, err := s.openRowsWithRetry(ctx)
				if err != nil {
					return nil, nil, fmt.Errorf("executing query: %w", err)
				}
				s.rows = rows
				s.resetCursorCache()

			case shModeIncrementing, shModeTimestamp, shModeTimestampIncrementing:
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
				// Pre-warm schema cache before opening rows (same reason as bulk mode).
				if s.schemaName != "" {
					_, _ = s.schemas.schemaForEvent(ctx, s.schemaName, s.tableName, nil)
				}
				rows, err := s.openRowsWithRetry(ctx)
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
				_ = s.rows.Close()
				s.rows = nil
				s.resetCursorCache()
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
							_ = s.rows.Close()
							s.rows = nil
							s.resetCursorCache()
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
							_ = s.rows.Close()
							s.rows = nil
							s.resetCursorCache()
							return nil, nil, fmt.Errorf("iterating rows: %w", rErr)
						}
						_ = s.rows.Close()
						s.rows = nil
						s.resetCursorCache()
						s.hwmSafe = s.hwm
					}
					hwmSnap, tsHWMSnap := s.hwmSafe, s.timestampHWM
					return batch, func(ctx context.Context, err error) error {
						if err != nil {
							return nil
						}
						if saveErr := s.saveCheckpointValues(ctx, hwmSnap, tsHWMSnap); saveErr != nil {
							s.log.Warnf("Failed to save checkpoint: %v", saveErr)
						}
						return nil
					}, nil
				}
				hwmSnap, tsHWMSnap := s.hwm, s.timestampHWM
				return batch, func(ctx context.Context, err error) error {
					if err != nil {
						return nil
					}
					if saveErr := s.saveCheckpointValues(ctx, hwmSnap, tsHWMSnap); saveErr != nil {
						s.log.Warnf("Failed to save checkpoint: %v", saveErr)
					}
					return nil
				}, nil
			}
		}
		if err := s.rows.Err(); err != nil {
			_ = s.rows.Close()
			s.rows = nil
			s.resetCursorCache()
			return nil, nil, fmt.Errorf("iterating rows: %w", err)
		}
		_ = s.rows.Close()
		s.rows = nil
		s.resetCursorCache()

		if s.mode == shModeTimestamp || s.mode == shModeTimestampIncrementing {
			s.timestampHWM = s.tsQueryUpper
		}
		if s.mode == shModeIncrementing {
			// Cursor fully consumed: every row seen, so current HWM is safe.
			s.hwmSafe = s.hwm
		}
		if s.mode == shModeBulk || s.mode == shModeQuery {
			s.bulkExhausted = true
		}

		if len(batch) > 0 {
			hwmSnap, tsHWMSnap := s.hwm, s.timestampHWM
			if s.mode == shModeIncrementing {
				hwmSnap = s.hwmSafe
			}
			return batch, func(ctx context.Context, err error) error {
				if err != nil {
					return nil
				}
				if saveErr := s.saveCheckpointValues(ctx, hwmSnap, tsHWMSnap); saveErr != nil {
					s.log.Warnf("Failed to save checkpoint: %v", saveErr)
				}
				return nil
			}, nil
		}

		// Empty poll: nothing in flight, safe to persist immediately.
		if err := s.saveCheckpoint(ctx); err != nil {
			s.log.Warnf("Failed to save checkpoint: %v", err)
		}

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

// normalizeHANAValue converts go-hdb-specific types to JSON-friendly Go types.
// NVARCHAR/VARCHAR arrive as []byte off the wire; DECIMAL as gohdb.Decimal (big.Rat alias).
// colType carries schema metadata for the column (nil when schema is unavailable).
// numericMapping controls how DECIMAL/NUMERIC values are emitted.
func normalizeHANAValue(v any, colType *schema.Common, numericMapping string) any {
	switch val := v.(type) {
	case []byte:
		return string(val)
	case gohdb.Decimal:
		return normalizeDecimal((*big.Rat)(&val), colType, numericMapping)
	case *gohdb.Decimal:
		if val == nil {
			return nil
		}
		return normalizeDecimal((*big.Rat)(val), colType, numericMapping)
	case *big.Rat:
		if val == nil {
			return nil
		}
		return normalizeDecimal(val, colType, numericMapping)
	}
	return v
}

// normalizeDecimal converts a big.Rat decimal to the representation selected
// by numericMapping.
func normalizeDecimal(r *big.Rat, colType *schema.Common, numericMapping string) any {
	// Determine the canonical form from schema type when available.
	if colType != nil {
		switch colType.Type {
		case schema.Int64:
			// DECIMAL(p,0) that fits in int64 — return the integer directly.
			if r.IsInt() && r.Num().IsInt64() {
				return r.Num().Int64()
			}
		case schema.Decimal:
			if colType.Logical != nil && colType.Logical.Decimal != nil {
				p := colType.Logical.Decimal.Precision
				s := colType.Logical.Decimal.Scale
				text := r.FloatString(int(s))
				if out, err := sqlutil.CanonicaliseDecimal(text, p, s); err == nil {
					return out
				}
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

// ratToNaturalDecimalString converts a *big.Rat to a decimal string using the
// minimal number of fractional digits that exactly represent the value.
// For HANA DECIMAL values the denominator is always a power of 10.
func ratToNaturalDecimalString(r *big.Rat) string {
	denom := new(big.Int).Set(r.Denom())
	ten := big.NewInt(10)
	scale := 0
	for denom.Cmp(big.NewInt(1)) > 0 {
		q, rem := new(big.Int).DivMod(denom, ten, new(big.Int))
		if rem.Sign() != 0 {
			return r.FloatString(38)
		}
		denom = q
		scale++
	}
	return r.FloatString(scale)
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
	}

	if err := rows.Scan(s.rowPtrs...); err != nil {
		return nil, fmt.Errorf("scanning columns: %w", err)
	}

	rowMap := make(map[string]any, len(s.rowColNames))
	for i, name := range s.rowColNames {
		var ct *schema.Common
		if s.rowCachedColTypes != nil {
			if c, ok := s.rowCachedColTypes[name]; ok {
				ct = &c
			}
		}
		rowMap[name] = normalizeHANAValue(s.rowValues[i], ct, s.numericMapping)
	}

	if (s.mode == shModeIncrementing || s.mode == shModeTimestampIncrementing) && s.incrementingCol != "" {
		if v, ok := rowMap[s.incrementingCol]; ok && v != nil {
			s.hwm = v
		}
	}

	b, err := json.Marshal(rowMap)
	if err != nil {
		return nil, fmt.Errorf("marshalling row: %w", err)
	}

	msg := service.NewMessage(b)

	if s.rowSchemaFetched {
		if s.rowCachedSchema != nil {
			msg.MetaSetMut("schema", s.rowCachedSchema)
		}
		if len(s.rowCachedPKCols) > 0 {
			if pkJSON, merr := json.Marshal(s.rowCachedPKCols); merr == nil {
				msg.MetaSetMut("primary_key_columns", string(pkJSON))
			}
		}
		msg.MetaSetMut("database_schema", s.schemaName)
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

func (s *sapHANAInput) loadCheckpoint(ctx context.Context) error {
	if s.checkpointCache == "" {
		return nil
	}
	var (
		raw    []byte
		getErr error
	)
	if err := s.mgr.AccessCache(ctx, s.checkpointCache, func(c service.Cache) {
		raw, getErr = c.Get(ctx, s.checkpointCacheKey)
	}); err != nil {
		return fmt.Errorf("accessing checkpoint cache %q: %w", s.checkpointCache, err)
	}
	if errors.Is(getErr, service.ErrKeyNotFound) {
		return nil
	}
	if getErr != nil {
		return fmt.Errorf("reading checkpoint key %q: %w", s.checkpointCacheKey, getErr)
	}

	var cp sapHANACheckpointState
	if err := json.Unmarshal(raw, &cp); err != nil {
		return fmt.Errorf("parsing checkpoint: %w", err)
	}
	if cp.TimestampHWM != nil {
		s.timestampHWM = *cp.TimestampHWM
	}
	switch {
	case cp.IncrHWMStr != nil:
		s.hwm = *cp.IncrHWMStr
	case cp.IncrHWMInt != nil:
		s.hwm = *cp.IncrHWMInt
	case cp.IncrHWMFloat != nil:
		s.hwm = *cp.IncrHWMFloat
	case cp.IncrHWMTime != nil:
		s.hwm = *cp.IncrHWMTime
	}
	s.log.Debugf("Loaded checkpoint: ts_hwm=%v incr_hwm=%v", s.timestampHWM, s.hwm)
	return nil
}

func (s *sapHANAInput) saveCheckpoint(ctx context.Context) error {
	if s.checkpointCache == "" {
		return nil
	}
	cp := sapHANACheckpointState{}
	if !s.timestampHWM.IsZero() {
		cp.TimestampHWM = &s.timestampHWM
	}
	if s.hwm != nil {
		switch v := s.hwm.(type) {
		case string:
			cp.IncrHWMStr = &v
		case int64:
			cp.IncrHWMInt = &v
		case float64:
			cp.IncrHWMFloat = &v
		case time.Time:
			cp.IncrHWMTime = &v
		}
	}
	b, err := json.Marshal(cp)
	if err != nil {
		return fmt.Errorf("marshalling checkpoint: %w", err)
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
	return nil
}

// saveCheckpointValues persists explicit HWM values to the cache. Used by
// AckFunc closures that capture snapshots at batch-return time so the cache
// write happens only after downstream acks, not at read time.
func (s *sapHANAInput) saveCheckpointValues(ctx context.Context, hwm any, tsHWM time.Time) error {
	if s.checkpointCache == "" {
		return nil
	}
	cp := sapHANACheckpointState{}
	if !tsHWM.IsZero() {
		cp.TimestampHWM = &tsHWM
	}
	if hwm != nil {
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
	}
	b, err := json.Marshal(cp)
	if err != nil {
		return fmt.Errorf("marshalling checkpoint: %w", err)
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
	return nil
}

func (s *sapHANAInput) Close(ctx context.Context) error {
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
