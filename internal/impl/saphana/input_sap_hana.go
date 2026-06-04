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
	"fmt"
	"math/big"
	"sync"
	"time"

	gohdb "github.com/SAP/go-hdb/driver"

	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/redpanda-data/connect/v4/internal/license"
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
		Description("SAP HANA connection DSN.").
		Example("hdb://user:password@host:39017"),
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
		Description("Column to use as the high-water mark for `incrementing` mode. Must be monotonically increasing (e.g. an auto-increment ID or a timestamp column).").
		Optional(),
	).
	Field(service.NewStringField(shFieldIncrementingInitialVal).
		Description("Initial high-water mark value. When empty, all existing rows are emitted on the first run.").
		Default(""),
	).
	Field(service.NewDurationField(shFieldPollInterval).
		Description("How long to wait between polls in `incrementing`, `timestamp`, and `timestamp+incrementing` modes.").
		Default("5s").
		Example("1s").
		Example("30s"),
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
	hwm             string
	pollInterval    time.Duration

	timestampCol   string
	timestampHWM   time.Time
	tsQueryUpper   time.Time
	timestampDelay time.Duration

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
	rowSchemaFetched bool
}

func newSAPHANAInput(conf *service.ParsedConfig, mgr *service.Resources) (*sapHANAInput, error) {
	if err := license.CheckRunningEnterprise(mgr); err != nil {
		return nil, err
	}

	s := &sapHANAInput{
		log:      mgr.Logger(),
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
	if s.hwm, err = conf.FieldString(shFieldIncrementingInitialVal); err != nil {
		return nil, err
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
	s.log.Debug("Connected to SAP HANA.")
	return nil
}

// tableRef returns a quoted table reference: "SCHEMA"."TABLE" or just "TABLE".
func (s *sapHANAInput) tableRef() string {
	if s.schemaName != "" {
		return `"` + s.schemaName + `"."` + s.tableName + `"`
	}
	return `"` + s.tableName + `"`
}

// openRows executes the query for the current mode and returns the result set.
func (s *sapHANAInput) openRows(ctx context.Context) (*sql.Rows, error) {
	switch s.mode {
	case shModeBulk:
		q := `SELECT * FROM ` + s.tableRef()
		return s.db.QueryContext(ctx, q)

	case shModeIncrementing:
		if s.hwm == "" {
			q := `SELECT * FROM ` + s.tableRef() +
				` ORDER BY "` + s.incrementingCol + `"`
			return s.db.QueryContext(ctx, q)
		}
		q := `SELECT * FROM ` + s.tableRef() +
			` WHERE "` + s.incrementingCol + `" > ? ORDER BY "` + s.incrementingCol + `"`
		return s.db.QueryContext(ctx, q, s.hwm)

	case shModeQuery:
		return s.db.QueryContext(ctx, s.customQuery)

	case shModeTimestamp:
		s.tsQueryUpper = time.Now().Add(-s.timestampDelay)
		if s.timestampHWM.IsZero() {
			q := `SELECT * FROM ` + s.tableRef() +
				` WHERE "` + s.timestampCol + `" <= ? ORDER BY "` + s.timestampCol + `"`
			return s.db.QueryContext(ctx, q, s.tsQueryUpper)
		}
		q := `SELECT * FROM ` + s.tableRef() +
			` WHERE "` + s.timestampCol + `" > ? AND "` + s.timestampCol + `" <= ? ORDER BY "` + s.timestampCol + `"`
		return s.db.QueryContext(ctx, q, s.timestampHWM, s.tsQueryUpper)

	case shModeTimestampIncrementing:
		s.tsQueryUpper = time.Now().Add(-s.timestampDelay)
		if s.timestampHWM.IsZero() {
			q := `SELECT * FROM ` + s.tableRef() +
				` WHERE "` + s.timestampCol + `" <= ?` +
				` ORDER BY "` + s.timestampCol + `", "` + s.incrementingCol + `"`
			return s.db.QueryContext(ctx, q, s.tsQueryUpper)
		}
		q := `SELECT * FROM ` + s.tableRef() +
			` WHERE ("` + s.timestampCol + `" > ? OR ("` + s.timestampCol + `" = ? AND "` + s.incrementingCol + `" > ?))` +
			` AND "` + s.timestampCol + `" <= ?` +
			` ORDER BY "` + s.timestampCol + `", "` + s.incrementingCol + `"`
		return s.db.QueryContext(ctx, q, s.timestampHWM, s.timestampHWM, s.hwm, s.tsQueryUpper)

	default:
		return nil, fmt.Errorf("unknown mode %q", s.mode)
	}
}

func (s *sapHANAInput) resetCursorCache() {
	s.rowColNames = nil
	s.rowValues = nil
	s.rowPtrs = nil
	s.rowCachedSchema = nil
	s.rowSchemaFetched = false
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

	noop := func(context.Context, error) error { return nil }

	for {
		if s.rows == nil {
			switch s.mode {
			case shModeBulk, shModeQuery:
				// Pre-warm schema cache before opening rows so the schema
				// query doesn't compete with the main query for a HANA connection.
				if s.schemaName != "" && s.mode != shModeQuery {
					_, _ = s.schemas.schemaForEvent(ctx, s.schemaName, s.tableName, nil)
				}
				rows, err := s.openRows(ctx)
				if err != nil {
					return nil, nil, fmt.Errorf("executing query: %w", err)
				}
				s.rows = rows
				s.resetCursorCache()

			case shModeIncrementing, shModeTimestamp, shModeTimestampIncrementing:
				// Release the lock while waiting so Close() can proceed.
				s.dbMut.Unlock()
				var timerFired bool
				select {
				case <-ctx.Done():
					s.dbMut.Lock()
					return nil, nil, ctx.Err()
				case <-s.stopChan:
					s.dbMut.Lock()
					return nil, nil, service.ErrEndOfInput
				case <-time.After(s.pollInterval):
					timerFired = true
				}
				s.dbMut.Lock()
				if !timerFired {
					return nil, nil, service.ErrEndOfInput
				}
				// Pre-warm schema cache before opening rows (same reason as bulk mode).
				if s.schemaName != "" {
					_, _ = s.schemas.schemaForEvent(ctx, s.schemaName, s.tableName, nil)
				}
				rows, err := s.openRows(ctx)
				if err != nil {
					return nil, nil, fmt.Errorf("executing query: %w", err)
				}
				s.rows = rows
				s.resetCursorCache()
			}
		}

		batch := make(service.MessageBatch, 0, s.fetchSize)
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
				return batch, noop, nil
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
		if s.mode == shModeBulk || s.mode == shModeQuery {
			s.bulkExhausted = true
		}

		if len(batch) > 0 {
			return batch, noop, nil
		}

		if s.bulkExhausted {
			return nil, nil, service.ErrEndOfInput
		}
	}
}

// normalizeHANAValue converts go-hdb-specific types to JSON-friendly Go types.
// NVARCHAR/VARCHAR arrive as []byte off the wire; DECIMAL as gohdb.Decimal (big.Rat alias).
func normalizeHANAValue(v any) any {
	switch val := v.(type) {
	case []byte:
		return string(val)
	case gohdb.Decimal:
		f, _ := (*big.Rat)(&val).Float64()
		return f
	case *gohdb.Decimal:
		if val == nil {
			return nil
		}
		f, _ := (*big.Rat)(val).Float64()
		return f
	case *big.Rat:
		if val == nil {
			return nil
		}
		f, _ := val.Float64()
		return f
	}
	return v
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
			schemaVal, _ := s.schemas.schemaForEvent(ctx, s.schemaName, s.tableName, colNames)
			s.rowCachedSchema = schemaVal
			s.rowSchemaFetched = true
		}
	}

	if err := rows.Scan(s.rowPtrs...); err != nil {
		return nil, fmt.Errorf("scanning columns: %w", err)
	}

	rowMap := make(map[string]any, len(s.rowColNames))
	for i, name := range s.rowColNames {
		rowMap[name] = normalizeHANAValue(s.rowValues[i])
	}

	if (s.mode == shModeIncrementing || s.mode == shModeTimestampIncrementing) && s.incrementingCol != "" {
		if v, ok := rowMap[s.incrementingCol]; ok && v != nil {
			s.hwm = fmt.Sprintf("%v", v)
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
		msg.MetaSetMut("sap_hana_schema", s.schemaName)
		msg.MetaSetMut("sap_hana_table", s.tableName)
	}

	return msg, nil
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
