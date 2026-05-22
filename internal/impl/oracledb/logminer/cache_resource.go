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
// cache resource.
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
func (c *ConnectCacheResource) StartTransaction(ctx context.Context, txnID sqlredo.TransactionID, scn uint64) error {
	if _, discarded := c.discarded[txnID]; discarded {
		return nil
	}

	var (
		existing *serializedTransactionMetadata
		err      error
	)
	if existing, err = c.readMetadata(ctx, txnID); err != nil {
		return fmt.Errorf("checking for existing transaction %s: %w", txnID, err)
	} else if existing != nil {
		return nil
	}

	metaData := &serializedTransactionMetadata{ID: txnID, SCN: scn, EventCount: 0}
	data, err := json.Marshal(metaData)
	if err != nil {
		return fmt.Errorf("serializing transaction metadata %s: %w", txnID, err)
	}

	var setErr error
	if err := c.resources.AccessCache(ctx, c.cacheName, func(cache service.Cache) {
		setErr = cache.Set(ctx, c.toMetaKey(txnID), data, nil)
	}); err != nil {
		return fmt.Errorf("accessing cache for transaction %s: %w", txnID, err)
	}
	if setErr != nil {
		return fmt.Errorf("storing transaction metadata %s: %w", txnID, setErr)
	}
	c.transactionsMetric.Incr(1)
	return nil
}

// AddEvent writes a DML event to its own cache key and updates the transaction
// metadata counter. Each call is O(1): one constant-size event key write plus
// one constant-size metadata key update, regardless of how many events the
// transaction has already accumulated.
func (c *ConnectCacheResource) AddEvent(ctx context.Context, txnID sqlredo.TransactionID, scn uint64, event *sqlredo.DMLEvent) error {
	if _, discarded := c.discarded[txnID]; discarded {
		return nil
	}

	var (
		m   *serializedTransactionMetadata
		err error
	)
	if m, err = c.readMetadata(ctx, txnID); err != nil {
		return fmt.Errorf("reading transaction metadata %s: %w", txnID, err)
	}

	if m == nil {
		c.log.Warnf("Transaction %s not found for event, creating...", txnID)
		m = &serializedTransactionMetadata{ID: txnID, SCN: scn, EventCount: 0}
		c.startSCNs[txnID] = scn
		c.transactionsMetric.Incr(1)
	} else if m.EventCount == 0 {
		c.startSCNs[txnID] = m.SCN
	}

	evData, err := marshalEvent(event)
	if err != nil {
		return fmt.Errorf("serializing event for transaction %s: %w", txnID, err)
	}
	var setErr error
	if err := c.resources.AccessCache(ctx, c.cacheName, func(cache service.Cache) {
		setErr = cache.Set(ctx, c.toEventKey(txnID, m.EventCount), evData, nil)
	}); err != nil {
		return fmt.Errorf("accessing cache for event %d of transaction %s: %w", m.EventCount, txnID, err)
	}
	if setErr != nil {
		return fmt.Errorf("writing event %d for transaction %s: %w", m.EventCount, txnID, setErr)
	}

	m.EventCount++
	c.eventsMetric.Incr(1)

	if c.maxEvents > 0 && m.EventCount > c.maxEvents {
		c.log.Warnf("Transaction %s exceeded max event buffer of %d events, discarding", txnID, c.maxEvents)
		c.eventsMetric.Decr(int64(m.EventCount))
		c.deleteTransactionKeys(ctx, txnID, m.EventCount)
		delete(c.startSCNs, txnID)
		c.transactionsMetric.Decr(1)
		c.discarded[txnID] = struct{}{}
		return nil
	}

	metaData, err := json.Marshal(m)
	if err != nil {
		return fmt.Errorf("serializing updated metadata for transaction %s: %w", txnID, err)
	}

	var updateErr error
	if err := c.resources.AccessCache(ctx, c.cacheName, func(cache service.Cache) {
		updateErr = cache.Set(ctx, c.toMetaKey(txnID), metaData, nil)
	}); err != nil {
		return fmt.Errorf("accessing cache to update metadata for transaction %s: %w", txnID, err)
	}
	if updateErr != nil {
		return fmt.Errorf("updating metadata for transaction %s: %w", txnID, updateErr)
	}
	return nil
}

// GetTransaction retrieves the transaction with the given ID by reading the
// metadata key for the event count and then fetching each event key in order.
// Returns (nil, nil) if the transaction is not found.
// Returns (nil, err) on a cache backend error or unmarshal failure.
func (c *ConnectCacheResource) GetTransaction(ctx context.Context, txnID sqlredo.TransactionID) (*Transaction, error) {
	var (
		m   *serializedTransactionMetadata
		err error
	)
	if m, err = c.readMetadata(ctx, txnID); err != nil {
		return nil, fmt.Errorf("reading transaction metadata %s: %w", txnID, err)
	}
	if m == nil {
		return nil, nil
	}

	events := make([]*sqlredo.DMLEvent, m.EventCount)
	var readErr error
	if err := c.resources.AccessCache(ctx, c.cacheName, func(cache service.Cache) {
		for i := range m.EventCount {
			data, err := cache.Get(ctx, c.toEventKey(txnID, i))
			if err != nil {
				if errors.Is(err, service.ErrKeyNotFound) {
					readErr = fmt.Errorf("event %d of transaction %s not found (cache eviction?)", i, txnID)
				} else {
					readErr = fmt.Errorf("reading event %d of transaction %s: %w", i, txnID, err)
				}
				return
			}
			ev, err := unmarshalEvent(data)
			if err != nil {
				readErr = fmt.Errorf("deserializing event %d of transaction %s: %w", i, txnID, err)
				return
			}
			events[i] = ev
		}
	}); err != nil {
		return nil, err
	}
	if readErr != nil {
		return nil, readErr
	}

	return &Transaction{ID: m.ID, SCN: m.SCN, Events: events}, nil
}

// CommitTransaction removes the committed transaction from the cache.
func (c *ConnectCacheResource) CommitTransaction(ctx context.Context, txnID sqlredo.TransactionID) error {
	delete(c.discarded, txnID)
	delete(c.startSCNs, txnID)

	var (
		m   *serializedTransactionMetadata
		err error
	)
	if m, err = c.readMetadata(ctx, txnID); err != nil {
		return fmt.Errorf("reading metadata for committing transaction %s: %w", txnID, err)
	} else if m == nil {
		return nil
	}
	c.eventsMetric.Decr(int64(m.EventCount))
	c.deleteTransactionKeys(ctx, txnID, m.EventCount)
	c.transactionsMetric.Decr(1)

	return nil
}

// RollbackTransaction removes the rolled-back transaction from the cache,
// discarding all buffered events.
func (c *ConnectCacheResource) RollbackTransaction(ctx context.Context, txnID sqlredo.TransactionID) error {
	delete(c.discarded, txnID)
	delete(c.startSCNs, txnID)

	var (
		meta *serializedTransactionMetadata
		err  error
	)
	if meta, err = c.readMetadata(ctx, txnID); err != nil {
		return fmt.Errorf("reading metadata for rolling back transaction %s: %w", txnID, err)
	} else if meta == nil {
		return nil
	}
	c.eventsMetric.Decr(int64(meta.EventCount))
	c.deleteTransactionKeys(ctx, txnID, meta.EventCount)
	c.transactionsMetric.Decr(1)

	return nil
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

func (c *ConnectCacheResource) toMetaKey(txnID sqlredo.TransactionID) string {
	return c.keyPrefix + ":meta:" + string(txnID)
}

func (c *ConnectCacheResource) toEventKey(txnID sqlredo.TransactionID, seqno int) string {
	return c.keyPrefix + ":evt:" + string(txnID) + ":" + strconv.Itoa(seqno)
}

// readMetadata fetches and deserializes the metadata key for txnID.
// Returns (nil, nil) if the key does not exist.
func (c *ConnectCacheResource) readMetadata(ctx context.Context, txnID sqlredo.TransactionID) (*serializedTransactionMetadata, error) {
	var (
		meta   *serializedTransactionMetadata
		getErr error
	)
	if err := c.resources.AccessCache(ctx, c.cacheName, func(cache service.Cache) {
		data, err := cache.Get(ctx, c.toMetaKey(txnID))
		if err != nil {
			getErr = err
			return
		}
		meta, getErr = unmarshalMetadata(data)
	}); err != nil {
		return nil, err
	}
	if getErr != nil {
		if errors.Is(getErr, service.ErrKeyNotFound) {
			return nil, nil
		}
		return nil, getErr
	}
	return meta, nil
}

// deleteTransactionKeys deletes all event keys (0..eventCount-1) plus the
// metadata key for txnID in a single AccessCache call.
func (c *ConnectCacheResource) deleteTransactionKeys(ctx context.Context, txnID sqlredo.TransactionID, eventCount int) {
	if err := c.resources.AccessCache(ctx, c.cacheName, func(cache service.Cache) {
		for i := range eventCount {
			if err := cache.Delete(ctx, c.toEventKey(txnID, i)); err != nil && !errors.Is(err, service.ErrKeyNotFound) {
				c.log.Errorf("Failed to delete event key %d for transaction %s: %v", i, txnID, err)
			}
		}
		if err := cache.Delete(ctx, c.toMetaKey(txnID)); err != nil && !errors.Is(err, service.ErrKeyNotFound) {
			c.log.Errorf("Failed to delete metadata key for transaction %s: %v", txnID, err)
		}
	}); err != nil {
		c.log.Errorf("Failed to access cache to delete transaction %s keys: %v", txnID, err)
	}
}

// Serialise DLMEvent.Data and OldValues to ensure the types survive
// plain encoding/json roundtrip by wrapping them in a typedVal.

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
	var (
		val string
		ok  bool
	)
	switch tv.T {
	case typeNull:
		return nil, nil
	case typeString:
		if val, ok = tv.V.(string); !ok {
			return nil, fmt.Errorf("expected string, got %T", tv.V)
		}
		return val, nil
	case typeInt64:
		if val, ok = tv.V.(string); !ok {
			return nil, fmt.Errorf("expected string for int64, got %T", tv.V)
		}
		return strconv.ParseInt(val, 10, 64)
	case typeNumber:
		if val, ok = tv.V.(string); !ok {
			return nil, fmt.Errorf("expected string for json.Number, got %T", tv.V)
		}
		return json.Number(val), nil
	case typeBytes:
		if val, ok = tv.V.(string); !ok {
			return nil, fmt.Errorf("expected string for bytes, got %T", tv.V)
		}
		return base64.StdEncoding.DecodeString(val)
	case typeTime:
		if val, ok = tv.V.(string); !ok {
			return nil, fmt.Errorf("expected string for time, got %T", tv.V)
		}
		return time.Parse(time.RFC3339Nano, val)
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

type serializedTransactionMetadata struct {
	ID         sqlredo.TransactionID `json:"id"`
	SCN        uint64                `json:"scn"`
	EventCount int                   `json:"count"`
}

func unmarshalMetadata(data []byte) (*serializedTransactionMetadata, error) {
	var m serializedTransactionMetadata
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, err
	}
	return &m, nil
}

func marshalEvent(ev *sqlredo.DMLEvent) ([]byte, error) {
	se := serializedDMLEvent{
		Operation:     ev.Operation,
		Schema:        ev.Schema,
		Table:         ev.Table,
		SQLRedo:       ev.SQLRedo,
		Data:          encodeMap(ev.Data),
		OldValues:     encodeMap(ev.OldValues),
		Timestamp:     ev.Timestamp,
		TransactionID: ev.TransactionID,
	}
	return json.Marshal(se)
}

func unmarshalEvent(data []byte) (*sqlredo.DMLEvent, error) {
	var se serializedDMLEvent
	if err := json.Unmarshal(data, &se); err != nil {
		return nil, err
	}
	dataMap, err := decodeMap(se.Data)
	if err != nil {
		return nil, fmt.Errorf("event data: %w", err)
	}
	oldMap, err := decodeMap(se.OldValues)
	if err != nil {
		return nil, fmt.Errorf("event old_values: %w", err)
	}
	return &sqlredo.DMLEvent{
		Operation:     se.Operation,
		Schema:        se.Schema,
		Table:         se.Table,
		SQLRedo:       se.SQLRedo,
		Data:          dataMap,
		OldValues:     oldMap,
		Timestamp:     se.Timestamp,
		TransactionID: se.TransactionID,
	}, nil
}
