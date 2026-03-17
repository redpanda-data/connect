// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package logminer

import (
	"math"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/connect/v4/internal/impl/oracledb/logminer/sqlredo"
)

// TransactionCache is responsible for buffering transactions until a commit event is received,
// at which point we know it's safe to flush transactions to the Connect pipeline.
// If a rollback events is received the cache will be be cleared instead of flushed.
type TransactionCache interface {
	StartTransaction(txnID string, scn uint64)
	AddEvent(txnID string, scn uint64, event *sqlredo.DMLEvent)
	GetTransaction(txnID string) *Transaction
	CommitTransaction(txnID string)
	RollbackTransaction(txnID string)
}

// TransactionID uniquely identifies an Oracle database transaction.
type TransactionID string

// Transaction buffers events until commit
type Transaction struct {
	ID     string
	SCN    uint64
	Events []*sqlredo.DMLEvent
}

// InMemoryCache is an in-memory implementation of TransactionCache that stores
// transactions in a map. This cache is used to buffer DML events until a transaction
// commits or rolls back. All operations are sequential and not protected by locks.
type InMemoryCache struct {
	transactions         map[string]*Transaction
	discardedTxns        map[string]struct{}
	maxTransactionEvents int
	transactionsMetric   *service.MetricGauge
	eventsMetric         *service.MetricGauge
	log                  *service.Logger
}

// NewInMemoryCache creates a new in-memory transaction cache with the specified logger.
// The cache buffers transactions until they commit or rollback. maxTransactionEvents
// sets the maximum number of events per transaction before it is discarded; 0 disables the limit.
func NewInMemoryCache(maxTransactionEvents int, metrics *service.Metrics, logger *service.Logger) *InMemoryCache {
	return &InMemoryCache{
		transactions:         make(map[string]*Transaction),
		discardedTxns:        make(map[string]struct{}),
		maxTransactionEvents: maxTransactionEvents,
		transactionsMetric:   metrics.NewGauge("oracledb_cdc_transactions_active"),
		eventsMetric:         metrics.NewGauge("oracledb_cdc_transactions_events_inflight"),
		log:                  logger,
	}
}

// StartTransaction initializes a new transaction in the cache with the given transaction ID and SCN.
// If the transaction already exists in the cache it is left untouched so that previously
// accumulated events are not lost when LogMiner re-emits the START record across polling cycles.
func (tc *InMemoryCache) StartTransaction(txnID string, scn uint64) {
	if _, discarded := tc.discardedTxns[txnID]; discarded {
		return
	}
	if _, exists := tc.transactions[txnID]; exists {
		return
	}
	tc.transactions[txnID] = &Transaction{
		ID:     txnID,
		SCN:    scn,
		Events: []*sqlredo.DMLEvent{},
	}
	tc.transactionsMetric.Incr(1)
}

// AddEvent adds a DML event to the specified transaction's buffer.
// If the transaction doesn't exist, it creates a new transaction with the event.
// If maxTransactionEvents is set and the buffer exceeds it, the transaction is discarded.
func (tc *InMemoryCache) AddEvent(txnID string, scn uint64, event *sqlredo.DMLEvent) {
	if _, discarded := tc.discardedTxns[txnID]; discarded {
		return
	}
	if txn, exists := tc.transactions[txnID]; exists {
		txn.Events = append(txn.Events, event)
		tc.eventsMetric.Incr(1)

		if tc.maxTransactionEvents > 0 && len(txn.Events) > tc.maxTransactionEvents {
			tc.log.Warnf("Transaction %s exceeded max event buffer of %d events, discarding", txnID, tc.maxTransactionEvents)
			tc.eventsMetric.Decr(int64(len(txn.Events)))
			delete(tc.transactions, txnID)
			tc.transactionsMetric.Decr(1)
			tc.discardedTxns[txnID] = struct{}{}
		}
	} else {
		// Transaction not started yet, create it. This is an edgecase that _shouldn't_ happen.
		tc.log.Warnf("Transaction %s not found for event, creating...", txnID)
		t := &Transaction{
			ID:     txnID,
			SCN:    scn,
			Events: []*sqlredo.DMLEvent{event},
		}
		tc.transactions[txnID] = t
		tc.transactionsMetric.Incr(1)
		tc.eventsMetric.Incr(1)
	}
}

// GetTransaction retrieves the transaction with the given ID from the cache.
// Returns nil if the transaction doesn't exist.
func (tc *InMemoryCache) GetTransaction(txnID string) *Transaction {
	return tc.transactions[txnID]
}

// CommitTransaction removes the committed transaction from the cache.
func (tc *InMemoryCache) CommitTransaction(txnID string) {
	delete(tc.discardedTxns, txnID)
	tx, ok := tc.transactions[txnID]
	if !ok {
		return
	}
	tc.eventsMetric.Decr(int64(len(tx.Events)))

	delete(tc.transactions, txnID)
	tc.transactionsMetric.Decr(1)
}

// LowWatermarkSCN returns the lowest start SCN among all currently open
// (uncommitted) transactions. Returns math.MaxUint64 if no open transactions.
// This behaviour is specific to in-memory caches and not part of the cache interface.
func (tc *InMemoryCache) LowWatermarkSCN(excludeTxnID string) uint64 {
	lowestOpenSCN := uint64(math.MaxUint64)
	for id, txn := range tc.transactions {
		if id != excludeTxnID {
			lowestOpenSCN = min(lowestOpenSCN, txn.SCN)
		}
	}
	return lowestOpenSCN
}

// RollbackTransaction removes the rolled back transaction from the cache, discarding all buffered events.
func (tc *InMemoryCache) RollbackTransaction(txnID string) {
	delete(tc.discardedTxns, txnID)
	tx, ok := tc.transactions[txnID]
	if !ok {
		return
	}
	tc.eventsMetric.Decr(int64(len(tx.Events)))

	delete(tc.transactions, txnID)
	tc.transactionsMetric.Decr(1)
}
