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
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/Jeffail/checkpoint"

	"github.com/redpanda-data/benthos/v4/public/service"
)

// sapHANACheckpointState is the JSON shape persisted to the cache.
// Typed pointer fields preserve the original Go type so bind parameters
// round-trip correctly without implicit string casts.
type sapHANACheckpointState struct {
	TimestampHWM *time.Time `json:"ts_hwm,omitempty"`
	IncrHWMStr   *string    `json:"incr_hwm_str,omitempty"`
	IncrHWMInt   *int64     `json:"incr_hwm_int,omitempty"`
	IncrHWMFloat *float64   `json:"incr_hwm_float,omitempty"`
	IncrHWMTime  *time.Time `json:"incr_hwm_time,omitempty"`
	IncrHWMBytes []byte     `json:"incr_hwm_bytes,omitempty"` // BINARY/VARBINARY keys, base64 on the wire
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
	case []byte:
		cp.IncrHWMBytes = v
	}
	return cp
}

// incrHWM returns the persisted incrementing HWM with its original Go type,
// and whether one was persisted at all.
func (cp *sapHANACheckpointState) incrHWM() (any, bool) {
	switch {
	case cp.IncrHWMStr != nil:
		return *cp.IncrHWMStr, true
	case cp.IncrHWMInt != nil:
		return *cp.IncrHWMInt, true
	case cp.IncrHWMFloat != nil:
		return *cp.IncrHWMFloat, true
	case cp.IncrHWMTime != nil:
		return *cp.IncrHWMTime, true
	case cp.IncrHWMBytes != nil:
		return cp.IncrHWMBytes, true
	}
	return nil, false
}

// checkpointer owns the persisted HWM: loading it on connect, ordering batch
// snapshots by delivery order, and writing the contiguous acknowledged
// frontier to the checkpoint cache.
type checkpointer struct {
	mgr       *service.Resources
	log       *service.Logger
	enabled   bool // checkpoint_cache is set and the mode has an HWM to track
	cacheName string
	cacheKey  string

	// mFailures counts checkpoint persist failures, otherwise only a log line.
	mFailures *service.MetricCounter

	// tracker orders checkpoint persistence by delivery order: a batch's HWM
	// snapshot is only persisted once every earlier batch has also resolved.
	tracker *checkpoint.Capped[*sapHANACheckpointState]
	// ackMut makes resolve-then-persist atomic so a lower checkpoint can never
	// overwrite a higher one when acks land concurrently.
	ackMut sync.Mutex
	// lastPersisted is the marshalled state most recently written to the
	// cache, used to skip writes that would not change it. Guarded by ackMut.
	lastPersisted []byte
}

func newCheckpointer(mgr *service.Resources, conf *sapHANAInputConfig) *checkpointer {
	return &checkpointer{
		mgr:       mgr,
		log:       mgr.Logger(),
		enabled:   conf.checkpointingEnabled(),
		cacheName: conf.checkpointCache,
		cacheKey:  conf.checkpointCacheKey,
		mFailures: mgr.Metrics().NewCounter("sap_hana_checkpoint_write_failures_total"),
		tracker:   checkpoint.NewCapped[*sapHANACheckpointState](int64(conf.checkpointLimit)),
	}
}

// load reads the persisted checkpoint state from the cache. It returns nil
// when checkpointing is disabled or nothing has been persisted yet.
func (c *checkpointer) load(ctx context.Context) (*sapHANACheckpointState, error) {
	if !c.enabled {
		return nil, nil
	}
	var (
		raw    []byte
		getErr error
	)
	if err := c.mgr.AccessCache(ctx, c.cacheName, func(cache service.Cache) {
		raw, getErr = cache.Get(ctx, c.cacheKey)
	}); err != nil {
		return nil, fmt.Errorf("accessing checkpoint cache %q: %w", c.cacheName, err)
	}
	if errors.Is(getErr, service.ErrKeyNotFound) {
		return nil, nil
	}
	if getErr != nil {
		return nil, fmt.Errorf("reading checkpoint key %q: %w", c.cacheKey, getErr)
	}

	var cp sapHANACheckpointState
	if err := json.Unmarshal(raw, &cp); err != nil {
		return nil, fmt.Errorf("parsing checkpoint: %w", err)
	}
	return &cp, nil
}

// track registers a batch's HWM snapshot with the ack-order tracker and
// returns the AckFunc that resolves its slot. The snapshot is only persisted
// once every earlier batch has also resolved, so out-of-order acks can never
// checkpoint past rows still in flight. Nacks resolve like acks: they are
// replayed by auto_replay_nacks (the default), and disabling that is a
// documented opt-in to DROP rejected messages, so the checkpoint must advance
// past them rather than pin the tracker (which would block at
// checkpoint_limit and stall the input permanently).
func (c *checkpointer) track(ctx context.Context, batchLen int, hwm any, tsHWM time.Time) (service.AckFunc, error) {
	resolve, err := c.tracker.Track(ctx, checkpointSnapshot(hwm, tsHWM), int64(batchLen))
	if err != nil {
		return nil, fmt.Errorf("tracking batch for checkpointing: %w", err)
	}
	return func(ctx context.Context, ackErr error) error {
		if ackErr != nil {
			c.log.Warnf("Advancing the checkpoint past a batch rejected downstream (auto_replay_nacks is disabled, so the rejected messages are dropped by contract): %v", ackErr)
		}
		// Resolve and persist under one lock so a lower checkpoint can never
		// overwrite a higher one when acks land concurrently.
		c.ackMut.Lock()
		defer c.ackMut.Unlock()
		cp := resolve()
		if cp == nil || *cp == nil {
			return nil
		}
		if saveErr := c.persist(ctx, *cp); saveErr != nil {
			c.mFailures.Incr(1)
			c.log.Warnf("Failed to save checkpoint: %v", saveErr)
		}
		return nil
	}, nil
}

// persist writes the checkpoint state to the configured cache. Callers hold
// ackMut.
func (c *checkpointer) persist(ctx context.Context, cp *sapHANACheckpointState) error {
	if !c.enabled {
		return nil
	}
	b, err := json.Marshal(cp)
	if err != nil {
		return fmt.Errorf("marshalling checkpoint: %w", err)
	}
	// Out-of-order acks behind a pending batch and idle empty polls resolve
	// to the same highest checkpoint again and again; only pay for a cache
	// write when the persisted state actually changes.
	if bytes.Equal(b, c.lastPersisted) {
		return nil
	}
	var setErr error
	if err := c.mgr.AccessCache(ctx, c.cacheName, func(cache service.Cache) {
		setErr = cache.Set(ctx, c.cacheKey, b, nil)
	}); err != nil {
		return fmt.Errorf("accessing checkpoint cache %q: %w", c.cacheName, err)
	}
	if setErr != nil {
		return fmt.Errorf("writing checkpoint key %q: %w", c.cacheKey, setErr)
	}
	c.lastPersisted = b
	return nil
}

// loadCheckpoint restores persisted HWM state from the cache. It reports
// whether an incrementing HWM was restored, since that value supersedes the
// configured initial value.
func (s *sapHANAInput) loadCheckpoint(ctx context.Context) (bool, error) {
	cp, err := s.cp.load(ctx)
	if err != nil || cp == nil {
		return false, err
	}
	if cp.TimestampHWM != nil {
		s.timestampHWM = *cp.TimestampHWM
	}
	hwm, resumedHWM := cp.incrHWM()
	if resumedHWM {
		s.hwm = hwm
	}
	s.log.Debugf("Loaded checkpoint: ts_hwm=%v incr_hwm=%v", s.timestampHWM, s.hwm)
	return resumedHWM, nil
}

// hwmEqual compares two high-water mark values. They are the normalised
// column values scanRow produces (int64, float64, string, time.Time, []byte
// or nil); a binary key arrives as []byte, and comparing two interfaces that
// hold slices with == panics, so that case is handled by content.
func hwmEqual(a, b any) bool {
	ab, aIsBytes := a.([]byte)
	bb, bIsBytes := b.([]byte)
	if aIsBytes || bIsBytes {
		return aIsBytes && bIsBytes && bytes.Equal(ab, bb)
	}
	return a == b
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

// resolveIncrementingInitialValue coerces the configured
// incrementing_initial_value to the incrementing column's catalog type. YAML
// only gives us a string, and go-hdb converts bind parameters client-side
// against the prepared statement's metadata: an int64 against an NVARCHAR
// key or a string against a TIMESTAMP is rejected on every poll, so the
// input would never progress. If the catalog is unreadable the constructor's
// heuristic guess is kept and the situation is logged.
func (s *sapHANAInput) resolveIncrementingInitialValue(ctx context.Context, db *sql.DB) error {
	dataType, err := fetchHANAColumnType(ctx, db, s.conf.schemaName, s.conf.tableName, s.conf.incrementingCol)
	if err != nil {
		s.log.Warnf("Could not determine the type of %s column %q from SYS.TABLE_COLUMNS, binding %s as %T: %v",
			shFieldIncrementingColumn, s.conf.incrementingCol, shFieldIncrementingInitialVal, s.hwm, err)
		return nil
	}
	v, err := coerceIncrementingValue(s.conf.incrInitialRaw, dataType)
	if err != nil {
		return fmt.Errorf("%s %q does not match %s %q of type %s: %w",
			shFieldIncrementingInitialVal, s.conf.incrInitialRaw, shFieldIncrementingColumn, s.conf.incrementingCol, dataType, err)
	}
	s.hwm = v
	return nil
}

// timestampColumnTypes are the catalog types a timestamp_column may have: the
// window predicate binds a time.Time against it and compares it with the
// database clock, which only makes sense for a timestamp-valued column.
var timestampColumnTypes = map[string]struct{}{
	"TIMESTAMP": {}, "LONGDATE": {}, "SECONDDATE": {},
}

// validateTimestampColumn checks timestamp_column against SYS.TABLE_COLUMNS
// so a wrongly typed column fails at connect time with a clear message rather
// than on the first poll's bind. An unreadable catalog is logged and skipped,
// as for the incrementing column.
func (s *sapHANAInput) validateTimestampColumn(ctx context.Context, db *sql.DB) error {
	dataType, err := fetchHANAColumnType(ctx, db, s.conf.schemaName, s.conf.tableName, s.conf.timestampCol)
	if err != nil {
		s.log.Warnf("Could not determine the type of %s column %q from SYS.TABLE_COLUMNS, continuing without checking it: %v",
			shFieldTimestampColumn, s.conf.timestampCol, err)
		return nil
	}
	if _, ok := timestampColumnTypes[dataType]; !ok {
		return fmt.Errorf("%s %q has type %s; %s modes need a TIMESTAMP, LONGDATE or SECONDDATE column",
			shFieldTimestampColumn, s.conf.timestampCol, dataType, s.conf.mode)
	}
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
	case "BINARY", "VARBINARY":
		// A binary key (ULID, UUID) is written in hex; binding the text itself
		// would compare against the ASCII bytes of the hex, not the key.
		b, err := hex.DecodeString(raw)
		if err != nil {
			return nil, fmt.Errorf("expected hexadecimal bytes: %w", err)
		}
		return b, nil
	case "DATE", "TIME", "TIMESTAMP", "SECONDDATE", "LONGDATE", "DAYDATE", "SECONDTIME":
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
