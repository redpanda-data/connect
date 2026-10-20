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
	"sync"
	"time"

	gohdb "github.com/SAP/go-hdb/driver"

	"github.com/redpanda-data/benthos/v4/public/schema"
	"github.com/redpanda-data/benthos/v4/public/service"
)

// rowKey is one row's (timestamp, incrementing) position, the unit the
// timestamp+incrementing tie-break predicate resumes from. A zero incr means
// the row had no usable key.
type rowKey struct {
	ts   time.Time
	incr any
}

type sapHANAInput struct {
	conf sapHANAInputConfig
	cp   *checkpointer

	hwm       any
	hwmSafe   any              // last checkpointable HWM: highest value whose tie-group is fully emitted
	peekedRow *service.Message // row buffered from peek-ahead at fetch_size boundary

	timestampHWM time.Time
	lastRowKey   rowKey // resume key of the most recently scanned row (timestamp+incrementing mid-window checkpoints)
	tsQueryUpper time.Time

	db      *sql.DB
	rows    *sql.Rows
	dbMut   sync.Mutex
	schemas *schemaCache
	log     *service.Logger

	stopChan chan struct{}
	stopOnce sync.Once

	// Low-cardinality operational signals: rows emitted, polls issued, query
	// retries, and the time each poll query takes to open. Checkpoint persist
	// failures are counted by the checkpointer.
	mRowsRead      *service.MetricCounter
	mPolls         *service.MetricCounter
	mQueryRetries  *service.MetricCounter
	mQueryDuration *service.MetricTimer

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

func (s *sapHANAInput) Connect(ctx context.Context) (err error) {
	s.dbMut.Lock()
	defer s.dbMut.Unlock()

	if s.db != nil {
		return nil
	}

	connector, connErr := gohdb.NewDSNConnector(s.conf.dsn)
	if connErr != nil {
		return fmt.Errorf("creating SAP HANA connector: %w", connErr)
	}
	connector.SetFetchSize(s.conf.fetchSize)
	db := sql.OpenDB(connector)
	// s.db is only assigned once every step below has succeeded, so a failed
	// Connect leaves the input disconnected and closes the half-opened pool.
	defer func() {
		if err != nil {
			_ = db.Close()
		}
	}()
	if err = db.PingContext(ctx); err != nil {
		return fmt.Errorf("pinging SAP HANA: %w", err)
	}

	resumedHWM, err := s.loadCheckpoint(ctx)
	if err != nil {
		return fmt.Errorf("loading checkpoint: %w", err)
	}
	// A persisted checkpoint already carries the HWM with its real type; only
	// a fresh start binds the configured initial value, which must match the
	// column's type or go-hdb rejects the parameter on every poll.
	if !resumedHWM && s.conf.incrInitialRaw != "" && s.conf.incrementingCol != "" {
		if err = s.resolveIncrementingInitialValue(ctx, db); err != nil {
			return err
		}
	}
	if s.conf.usesTimestamp() {
		if err = s.validateTimestampColumn(ctx, db); err != nil {
			return err
		}
	}
	// hwmSafe must start at the loaded checkpoint value so that a partial
	// batch on the first poll never persists nil and regresses progress.
	s.hwmSafe = s.hwm

	s.db = db
	s.schemas = newSchemaCache(db, s.log, s.conf.numericMapping)

	s.log.Debug("Connected to SAP HANA.")
	return nil
}

// openRows executes the query for the current mode and returns the result set.
func (s *sapHANAInput) openRows(ctx context.Context) (*sql.Rows, error) {
	switch s.conf.mode {
	case shModeBulk:
		q := `SELECT * FROM ` + s.conf.tableRef()
		return s.db.QueryContext(ctx, q)

	case shModeIncrementing:
		inc := quoteIdentifier(s.conf.incrementingCol)
		if s.hwm == nil {
			q := `SELECT * FROM ` + s.conf.tableRef() + ` ORDER BY ` + inc
			return s.db.QueryContext(ctx, q)
		}
		q := `SELECT * FROM ` + s.conf.tableRef() + ` WHERE ` + inc + ` > ? ORDER BY ` + inc
		return s.db.QueryContext(ctx, q, s.hwm)

	case shModeQuery:
		return s.db.QueryContext(ctx, s.conf.customQuery)

	case shModeTimestamp:
		tsc := quoteIdentifier(s.conf.timestampCol)
		upper, err := s.fetchWindowUpperBound(ctx)
		if err != nil {
			return nil, err
		}
		s.tsQueryUpper = upper
		if s.timestampHWM.IsZero() {
			q := `SELECT * FROM ` + s.conf.tableRef() + ` WHERE ` + tsc + ` <= ? ORDER BY ` + tsc
			return s.db.QueryContext(ctx, q, s.tsQueryUpper)
		}
		q := `SELECT * FROM ` + s.conf.tableRef() + ` WHERE ` + tsc + ` > ? AND ` + tsc + ` <= ? ORDER BY ` + tsc
		return s.db.QueryContext(ctx, q, s.timestampHWM, s.tsQueryUpper)

	case shModeTimestampIncrementing:
		tsc := quoteIdentifier(s.conf.timestampCol)
		inc := quoteIdentifier(s.conf.incrementingCol)
		upper, err := s.fetchWindowUpperBound(ctx)
		if err != nil {
			return nil, err
		}
		s.tsQueryUpper = upper
		if s.timestampHWM.IsZero() {
			q := `SELECT * FROM ` + s.conf.tableRef() + ` WHERE ` + tsc + ` <= ? ORDER BY ` + tsc + `, ` + inc
			return s.db.QueryContext(ctx, q, s.tsQueryUpper)
		}
		if s.hwm == nil {
			// timestampHWM advanced but no incrementing value seen yet (e.g. first window was empty).
			// Use pure timestamp comparison to avoid binding nil against a numeric column.
			q := `SELECT * FROM ` + s.conf.tableRef() +
				` WHERE ` + tsc + ` > ? AND ` + tsc + ` <= ?` +
				` ORDER BY ` + tsc + `, ` + inc
			return s.db.QueryContext(ctx, q, s.timestampHWM, s.tsQueryUpper)
		}
		q := `SELECT * FROM ` + s.conf.tableRef() +
			` WHERE (` + tsc + ` > ? OR (` + tsc + ` = ? AND ` + inc + ` > ?))` +
			` AND ` + tsc + ` <= ?` +
			` ORDER BY ` + tsc + `, ` + inc
		return s.db.QueryContext(ctx, q, s.timestampHWM, s.timestampHWM, s.hwm, s.tsQueryUpper)

	default:
		return nil, fmt.Errorf("unknown mode %q", s.conf.mode)
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
	if s.conf.timestampClock == shTimestampClockDatabaseUTC {
		q = hanaUTCClockQuery
	}
	var upper time.Time
	if err := s.db.QueryRowContext(ctx, q, -s.conf.timestampDelay.Seconds()).Scan(&upper); err != nil {
		return time.Time{}, fmt.Errorf("reading database clock for the poll window: %w", err)
	}
	return upper, nil
}

// openRowsWithRetry calls openRows, retrying up to s.conf.maxRetries times on
// failure with linear backoff. The lock is released during each backoff sleep
// so that Close() can proceed.
func (s *sapHANAInput) openRowsWithRetry(ctx context.Context) (*sql.Rows, error) {
	var err error
	for attempt := 0; attempt <= s.conf.maxRetries; attempt++ {
		if attempt > 0 {
			s.mQueryRetries.Incr(1)
			s.log.Warnf("Query failed (attempt %d/%d), retrying: %v", attempt, s.conf.maxRetries, err)
			s.dbMut.Unlock()
			select {
			case <-ctx.Done():
				s.dbMut.Lock()
				return nil, ctx.Err()
			case <-s.stopChan:
				s.dbMut.Lock()
				return nil, service.ErrEndOfInput
			case <-time.After(time.Duration(attempt) * s.conf.retryBackoff):
			}
			s.dbMut.Lock()
			if s.db == nil {
				return nil, service.ErrNotConnected
			}
		}
		var rows *sql.Rows
		started := time.Now()
		rows, err = s.openRows(ctx)
		s.mQueryDuration.Timing(time.Since(started).Nanoseconds())
		if err == nil {
			s.mPolls.Incr(1)
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
	if s.conf.usesIncrementing() {
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
			switch s.conf.mode {
			case shModeBulk, shModeQuery:
				// Pre-warm schema cache before opening rows so the schema
				// query doesn't compete with the main query for a HANA connection.
				if s.conf.schemaName != "" && s.conf.mode != shModeQuery {
					_, _ = s.schemas.schemaForEvent(ctx, s.conf.schemaName, s.conf.tableName, nil)
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
					case <-time.After(s.conf.pollInterval):
					}
					s.dbMut.Lock()
					if s.db == nil {
						return nil, nil, service.ErrNotConnected
					}
				}
				s.polledOnce = true
				// Pre-warm schema cache before opening rows (same reason as bulk mode).
				if s.conf.schemaName != "" {
					_, _ = s.schemas.schemaForEvent(ctx, s.conf.schemaName, s.conf.tableName, nil)
				}
				rows, err := s.openRowsWithRetry(ctx) //nolint:rowserrcheck // rows.Err is checked after iteration via s.rows
				if err != nil {
					return nil, nil, fmt.Errorf("executing query: %w", err)
				}
				s.rows = rows
				s.resetCursorCache()
			}
		}

		batch := make(service.MessageBatch, 0, s.conf.fetchSize)
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
			if len(batch) >= s.conf.fetchSize {
				if s.conf.mode == shModeIncrementing {
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
						if !hwmEqual(s.hwm, hwmAtPeek) {
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
				if s.conf.mode == shModeTimestampIncrementing {
					// Every scanned row is now handed to the framework (which
					// auto-replays nacks), so the cursor may safely resume from
					// the current HWM after an error.
					s.hwmSafe = s.hwm
					// Mid-window, checkpoint the last delivered row's
					// (timestamp, incrementing) pair: the tie-break predicate
					// resumes exactly after it, instead of re-reading the whole
					// window from its start on restart.
					if k := s.lastRowKey; k.incr != nil {
						return s.deliverBatchAt(ctx, batch, k.incr, k.ts)
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

		if s.conf.usesTimestamp() {
			s.timestampHWM = s.tsQueryUpper
		}
		if s.conf.usesIncrementing() {
			// Cursor fully consumed: every row seen, so current HWM is safe.
			s.hwmSafe = s.hwm
		}
		if s.conf.mode == shModeBulk || s.conf.mode == shModeQuery {
			s.bulkExhausted = true
		}

		if len(batch) > 0 {
			hwmSnap := s.hwm
			if s.conf.mode == shModeIncrementing {
				hwmSnap = s.hwmSafe
			}
			return s.deliverBatch(ctx, batch, hwmSnap)
		}

		// Empty poll: route the persist through the tracker so it stays
		// ordered behind batches still awaiting acks.
		hwmSnap := s.hwm
		if s.conf.usesIncrementing() {
			hwmSnap = s.hwmSafe
		}
		ackFn, err := s.cp.track(ctx, 0, hwmSnap, s.timestampHWM)
		if err != nil {
			return nil, nil, err
		}
		_ = ackFn(ctx, nil)

		if s.bulkExhausted {
			return nil, nil, service.ErrEndOfInput
		}
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
		if s.conf.mode != shModeQuery {
			sr, _ := s.schemas.schemaForEvent(ctx, s.conf.schemaName, s.conf.tableName, colNames)
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
		s.rowDriverColTypes = driverColumnTypes(colTypes, s.conf.numericMapping)
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
		v, err := normalizeHANAValue(s.rowValues[i], ct, s.conf.numericMapping)
		if err != nil {
			return nil, fmt.Errorf("normalising column %q: %w", name, err)
		}
		rowMap[name] = v
	}

	var incrVal any
	if s.conf.usesIncrementing() && s.conf.incrementingCol != "" {
		if v, ok := rowMap[s.conf.incrementingCol]; ok && v != nil {
			s.hwm = v
			incrVal = v
		}
	}
	if s.conf.mode == shModeTimestampIncrementing {
		// The mid-window resume key must be a single row's (timestamp,
		// incrementing) pair. A row with no usable incrementing value
		// invalidates it (rather than pairing its timestamp with an earlier
		// row's HWM, which would skip rows on resume) and the checkpoint
		// falls back to the window start.
		s.lastRowKey = rowKey{}
		if ts, ok := rowMap[s.conf.timestampCol].(time.Time); ok && incrVal != nil {
			s.lastRowKey = rowKey{ts: ts, incr: incrVal}
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
		if s.conf.schemaName != "" {
			msg.MetaSetMut("database_schema", s.conf.schemaName)
		}
		msg.MetaSetMut("table_name", s.conf.tableName)
	}

	return msg, nil
}

// deliverBatch registers the batch with the ack-order tracker and returns it
// alongside the AckFunc that resolves its checkpoint slot.
func (s *sapHANAInput) deliverBatch(ctx context.Context, batch service.MessageBatch, hwm any) (service.MessageBatch, service.AckFunc, error) {
	return s.deliverBatchAt(ctx, batch, hwm, s.timestampHWM)
}

// deliverBatchAt is deliverBatch with an explicit timestamp HWM snapshot.
func (s *sapHANAInput) deliverBatchAt(ctx context.Context, batch service.MessageBatch, hwm any, tsHWM time.Time) (service.MessageBatch, service.AckFunc, error) {
	ackFn, err := s.cp.track(ctx, len(batch), hwm, tsHWM)
	if err != nil {
		return nil, nil, err
	}
	s.mRowsRead.Incr(int64(len(batch)))
	return batch, ackFn, nil
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
