// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package logminer

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strconv"
	"time"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/connect/v4/internal/impl/oracledb/logminer/sqlredo"
)

// ConnectCacheResource is a TransactionCache that decorates a service.Cache
// resource, serializing in-flight transactions as JSON to offload the buffer
// from process memory. Use when large or long-running transactions would
// otherwise exhaust connector memory.
//
// resources.AccessCache is called per-operation so that the lookup goes
// through *service.Resources each time, matching the codebase convention for
// cache resources.
//
// The discarded-transaction set and the startSCNs index are kept in-memory;
// both are intentionally ephemeral and will be reconstructed on restart.
type ConnectCacheResource struct {
	resources          *service.Resources
	cacheName          string
	keyPrefix          string
	maxEvents          int
	discarded          map[sqlredo.TransactionID]struct{}
	log                *service.Logger
	transactionsMetric *service.MetricGauge
	eventsMetric       *service.MetricGauge
	// startSCNs tracks the start SCN of each open transaction. It is used by
	// LowWatermarkSCN to compute a safe checkpoint on commit without scanning
	// the external cache. Bounded by the number of concurrent open transactions.
	// Whilst strictly not needed for durable caches like Redis, it saves a little overhead
	// and is easier to apply to all caches than making them cache specific (given users can
	// configure in-memory cache as part of a cache_resource).
	startSCNs map[sqlredo.TransactionID]uint64
}

// NewConnectCacheResource creates a ConnectCacheResource backed by the named
// cache resource. keyPrefix is prepended to every cache key so that multiple
// oracledb_cdc inputs can share one cache resource without colliding — Oracle
// transaction IDs (USN.SLOT.SEQ) are only unique within a single Oracle instance.
func NewConnectCacheResource(resources *service.Resources, cfg TransactionCacheConfig, metrics *service.Metrics, log *service.Logger) *ConnectCacheResource {
	return &ConnectCacheResource{
		resources:          resources,
		cacheName:          cfg.CacheName,
		keyPrefix:          cfg.CacheKey,
		maxEvents:          cfg.MaxEvents,
		discarded:          make(map[sqlredo.TransactionID]struct{}),
		startSCNs:          make(map[sqlredo.TransactionID]uint64),
		log:                log,
		transactionsMetric: metrics.NewGauge("oracledb_cdc_transactions_active"),
		eventsMetric:       metrics.NewGauge("oracledb_cdc_transactions_events_inflight"),
	}
}

// StartTransaction initializes a new transaction in the cache. If the transaction
// already exists its accumulated events are left untouched.
func (c *ConnectCacheResource) StartTransaction(ctx context.Context, txnID sqlredo.TransactionID, scn uint64) {
	if _, discarded := c.discarded[txnID]; discarded {
		return
	}
	existing, err := c.GetTransaction(ctx, txnID)
	if err != nil {
		c.log.Errorf("Failed to check for existing transaction %s: %v", txnID, err)
		return
	}
	if existing != nil {
		return
	}
	txn := &Transaction{
		ID:     txnID,
		SCN:    scn,
		Events: []*sqlredo.DMLEvent{},
	}
	data, err := marshalTransaction(txn)
	if err != nil {
		c.log.Errorf("Failed to serialize transaction %s: %v", txnID, err)
		return
	}
	var setErr error
	if err := c.resources.AccessCache(ctx, c.cacheName, func(cache service.Cache) {
		setErr = cache.Set(ctx, c.toCacheKey(txnID), data, nil)
	}); err != nil {
		c.log.Errorf("Failed to access cache for transaction %s: %v", txnID, err)
		return
	}
	if setErr != nil {
		c.log.Errorf("Failed to store transaction %s in cache: %v", txnID, setErr)
		return
	}
	c.transactionsMetric.Incr(1)
}

// AddEvent appends a DML event to the named transaction's buffer. Each call
// incurs a cache Get followed by a cache Set (read-modify-write). This is safe
// because LogMiner processes events on a single goroutine.
func (c *ConnectCacheResource) AddEvent(ctx context.Context, txnID sqlredo.TransactionID, scn uint64, event *sqlredo.DMLEvent) {
	if _, discarded := c.discarded[txnID]; discarded {
		return
	}

	txn, err := c.GetTransaction(ctx, txnID)
	if err != nil {
		c.log.Errorf("Failed to get transaction %s, skipping event to avoid discarding buffered events: %v", txnID, err)
		return
	}
	if txn == nil {
		c.log.Warnf("Transaction %s not found for event, creating...", txnID)
		txn = &Transaction{
			ID:     txnID,
			SCN:    scn,
			Events: []*sqlredo.DMLEvent{event},
		}
		c.startSCNs[txnID] = scn
		c.transactionsMetric.Incr(1)
	} else {
		if len(txn.Events) == 0 {
			c.startSCNs[txnID] = txn.SCN
		}
		txn.Events = append(txn.Events, event)
	}
	c.eventsMetric.Incr(1)

	if c.maxEvents > 0 && len(txn.Events) > c.maxEvents {
		c.log.Warnf("Transaction %s exceeded max event buffer of %d events, discarding", txnID, c.maxEvents)
		c.eventsMetric.Decr(int64(len(txn.Events)))
		if err := c.resources.AccessCache(ctx, c.cacheName, func(cache service.Cache) {
			if err := cache.Delete(ctx, c.toCacheKey(txnID)); err != nil {
				c.log.Errorf("Failed to delete oversized transaction %s from cache: %v", txnID, err)
			}
		}); err != nil {
			c.log.Errorf("Failed to access cache for transaction %s: %v", txnID, err)
		}
		delete(c.startSCNs, txnID)
		c.transactionsMetric.Decr(1)
		c.discarded[txnID] = struct{}{}
		return
	}

	data, err := marshalTransaction(txn)
	if err != nil {
		c.log.Errorf("Failed to serialize transaction %s: %v", txnID, err)
		return
	}
	if err := c.resources.AccessCache(ctx, c.cacheName, func(cache service.Cache) {
		if err := cache.Set(ctx, c.toCacheKey(txnID), data, nil); err != nil {
			c.log.Errorf("Failed to update transaction %s in cache: %v", txnID, err)
		}
	}); err != nil {
		c.log.Errorf("Failed to access cache for transaction %s: %v", txnID, err)
	}
}

// GetTransaction retrieves the transaction with the given ID.
// Returns (nil, nil) if the key is not found.
// Returns (nil, err) on a cache backend error or unmarshal failure.
func (c *ConnectCacheResource) GetTransaction(ctx context.Context, txnID sqlredo.TransactionID) (*Transaction, error) {
	var (
		txn    *Transaction
		getErr error
	)
	if err := c.resources.AccessCache(ctx, c.cacheName, func(cache service.Cache) {
		var data []byte
		data, getErr = cache.Get(ctx, c.toCacheKey(txnID))
		if getErr != nil {
			return
		}
		txn, getErr = unmarshalTransaction(data)
	}); err != nil {
		return nil, err
	}
	if getErr != nil {
		if errors.Is(getErr, service.ErrKeyNotFound) {
			return nil, nil
		}
		return nil, getErr
	}
	return txn, nil
}

// CommitTransaction removes the committed transaction from the cache.
func (c *ConnectCacheResource) CommitTransaction(ctx context.Context, txnID sqlredo.TransactionID) {
	delete(c.discarded, txnID)
	delete(c.startSCNs, txnID)
	txn, err := c.GetTransaction(ctx, txnID)
	if err != nil {
		c.log.Errorf("Failed to get transaction %s during commit, event metrics may be inaccurate: %v", txnID, err)
	} else if txn == nil {
		return
	} else {
		c.eventsMetric.Decr(int64(len(txn.Events)))
	}
	if err := c.resources.AccessCache(ctx, c.cacheName, func(cache service.Cache) {
		if err := cache.Delete(ctx, c.toCacheKey(txnID)); err != nil {
			c.log.Errorf("Failed to delete committed transaction %s from cache: %v", txnID, err)
		}
	}); err != nil {
		c.log.Errorf("Failed to access cache for transaction %s: %v", txnID, err)
	}
	c.transactionsMetric.Decr(1)
}

// RollbackTransaction removes the rolled-back transaction from the cache,
// discarding all buffered events.
func (c *ConnectCacheResource) RollbackTransaction(ctx context.Context, txnID sqlredo.TransactionID) {
	delete(c.discarded, txnID)
	delete(c.startSCNs, txnID)
	txn, err := c.GetTransaction(ctx, txnID)
	if err != nil {
		c.log.Errorf("Failed to get transaction %s during rollback, event metrics may be inaccurate: %v", txnID, err)
	} else if txn == nil {
		return
	} else {
		c.eventsMetric.Decr(int64(len(txn.Events)))
	}
	if err := c.resources.AccessCache(ctx, c.cacheName, func(cache service.Cache) {
		if err := cache.Delete(ctx, c.toCacheKey(txnID)); err != nil {
			c.log.Errorf("Failed to delete rolled-back transaction %s from cache: %v", txnID, err)
		}
	}); err != nil {
		c.log.Errorf("Failed to access cache for transaction %s: %v", txnID, err)
	}
	c.transactionsMetric.Decr(1)
}

// LowWatermarkSCN returns the lowest start SCN among all currently open
// (uncommitted) transactions, excluding excludeTxnID. Returns math.MaxUint64
// if no other open transactions exist.
func (c *ConnectCacheResource) LowWatermarkSCN(excludeTxnID sqlredo.TransactionID) uint64 {
	lowestOpenSCN := uint64(math.MaxUint64)
	for id, scn := range c.startSCNs {
		if id != excludeTxnID {
			lowestOpenSCN = min(lowestOpenSCN, scn)
		}
	}
	return lowestOpenSCN
}

func (c *ConnectCacheResource) toCacheKey(txnID sqlredo.TransactionID) string {
	return c.keyPrefix + ":txn:" + string(txnID)
}

// ---------------------------------------------------------------------------
// Serialization
//
// DMLEvent.Data and OldValues hold map[string]any whose concrete types include
// time.Time, []byte, int64, json.Number, string, and nil — none of which
// survive a plain encoding/json round-trip through interface{} unchanged.
// Each value is wrapped in a typedVal envelope that carries a type tag so the
// original Go type is restored on decode.
// ---------------------------------------------------------------------------

const (
	typeString = "s"
	typeInt64  = "i"
	typeNumber = "n" // json.Number — stored as its string representation
	typeBytes  = "b" // base64-encoded
	typeTime   = "t" // RFC3339Nano
	typeNull   = "z"
)

type typedVal struct {
	T string `json:"t"`
	V any    `json:"v,omitempty"`
}

func encodeVal(v any) typedVal {
	switch val := v.(type) {
	case string:
		return typedVal{T: typeString, V: val}
	case int64:
		// Store as string to avoid float64 precision loss during JSON round-trip:
		// values above 2^53 cannot be represented exactly as float64.
		return typedVal{T: typeInt64, V: strconv.FormatInt(val, 10)}
	case json.Number:
		return typedVal{T: typeNumber, V: string(val)}
	case []byte:
		return typedVal{T: typeBytes, V: base64.StdEncoding.EncodeToString(val)}
	case time.Time:
		return typedVal{T: typeTime, V: val.Format(time.RFC3339Nano)}
	case nil:
		return typedVal{T: typeNull}
	default:
		// Fallback for any future types; loses fidelity but doesn't panic.
		return typedVal{T: typeString, V: fmt.Sprintf("%v", val)}
	}
}

func decodeVal(tv typedVal) (any, error) {
	switch tv.T {
	case typeNull:
		return nil, nil
	case typeString:
		s, ok := tv.V.(string)
		if !ok {
			return nil, fmt.Errorf("expected string, got %T", tv.V)
		}
		return s, nil
	case typeInt64:
		s, ok := tv.V.(string)
		if !ok {
			return nil, fmt.Errorf("expected string for int64, got %T", tv.V)
		}
		return strconv.ParseInt(s, 10, 64)
	case typeNumber:
		s, ok := tv.V.(string)
		if !ok {
			return nil, fmt.Errorf("expected string for json.Number, got %T", tv.V)
		}
		return json.Number(s), nil
	case typeBytes:
		s, ok := tv.V.(string)
		if !ok {
			return nil, fmt.Errorf("expected string for bytes, got %T", tv.V)
		}
		return base64.StdEncoding.DecodeString(s)
	case typeTime:
		s, ok := tv.V.(string)
		if !ok {
			return nil, fmt.Errorf("expected string for time, got %T", tv.V)
		}
		return time.Parse(time.RFC3339Nano, s)
	default:
		return nil, fmt.Errorf("unknown type tag %q", tv.T)
	}
}

type serializedMap map[string]typedVal

func encodeMap(m map[string]any) serializedMap {
	if m == nil {
		return nil
	}
	out := make(serializedMap, len(m))
	for k, v := range m {
		out[k] = encodeVal(v)
	}
	return out
}

func decodeMap(sm serializedMap) (map[string]any, error) {
	if sm == nil {
		return nil, nil
	}
	out := make(map[string]any, len(sm))
	for k, tv := range sm {
		v, err := decodeVal(tv)
		if err != nil {
			return nil, fmt.Errorf("decoding key %q: %w", k, err)
		}
		out[k] = v
	}
	return out, nil
}

type serializedDMLEvent struct {
	Operation     sqlredo.Operation     `json:"op"`
	Schema        string                `json:"schema"`
	Table         string                `json:"table"`
	SQLRedo       string                `json:"sql"`
	Data          serializedMap         `json:"data"`
	OldValues     serializedMap         `json:"old"`
	Timestamp     time.Time             `json:"ts"`
	TransactionID sqlredo.TransactionID `json:"txn_id"`
}

type serializedTransaction struct {
	ID     sqlredo.TransactionID `json:"id"`
	SCN    uint64                `json:"scn"`
	Events []serializedDMLEvent  `json:"events"`
}

func marshalTransaction(txn *Transaction) ([]byte, error) {
	st := serializedTransaction{
		ID:     txn.ID,
		SCN:    txn.SCN,
		Events: make([]serializedDMLEvent, len(txn.Events)),
	}
	for i, ev := range txn.Events {
		st.Events[i] = serializedDMLEvent{
			Operation:     ev.Operation,
			Schema:        ev.Schema,
			Table:         ev.Table,
			SQLRedo:       ev.SQLRedo,
			Data:          encodeMap(ev.Data),
			OldValues:     encodeMap(ev.OldValues),
			Timestamp:     ev.Timestamp,
			TransactionID: ev.TransactionID,
		}
	}
	return json.Marshal(st)
}

func unmarshalTransaction(data []byte) (*Transaction, error) {
	var st serializedTransaction
	if err := json.Unmarshal(data, &st); err != nil {
		return nil, err
	}
	txn := &Transaction{
		ID:     st.ID,
		SCN:    st.SCN,
		Events: make([]*sqlredo.DMLEvent, len(st.Events)),
	}
	for i, se := range st.Events {
		dataMap, err := decodeMap(se.Data)
		if err != nil {
			return nil, fmt.Errorf("event %d data: %w", i, err)
		}
		oldMap, err := decodeMap(se.OldValues)
		if err != nil {
			return nil, fmt.Errorf("event %d old_values: %w", i, err)
		}
		txn.Events[i] = &sqlredo.DMLEvent{
			Operation:     se.Operation,
			Schema:        se.Schema,
			Table:         se.Table,
			SQLRedo:       se.SQLRedo,
			Data:          dataMap,
			OldValues:     oldMap,
			Timestamp:     se.Timestamp,
			TransactionID: se.TransactionID,
		}
	}
	return txn, nil
}
