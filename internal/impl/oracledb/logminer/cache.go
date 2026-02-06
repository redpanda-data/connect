// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package logminer

import (
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
	transactions       map[string]*Transaction
	log                *service.Logger
	transactionsMetric *service.MetricGauge
	// eventsMetric       *service.MetricGauge
}

// NewInMemoryCache creates a new in-memory transaction cache with the specified logger.
// The cache buffers transactions until they commit or rollback.
func NewInMemoryCache(metrics *service.Metrics, logger *service.Logger) *InMemoryCache {
	return &InMemoryCache{
		transactions:       make(map[string]*Transaction),
		transactionsMetric: metrics.NewGauge("oracledb_cdc_active_transactions"),
		// eventsMetric:       metrics.NewGauge("oracledb_cdc_inflight_events"),
		log: logger,
	}
}

// StartTransaction initializes a new transaction in the cache with the given transaction ID and SCN.
func (tc *InMemoryCache) StartTransaction(txnID string, scn uint64) {
	tc.transactions[txnID] = &Transaction{
		ID:     txnID,
		SCN:    scn,
		Events: []*sqlredo.DMLEvent{},
	}
	tc.transactionsMetric.Incr(1)
}

// AddEvent adds a DML event to the specified transaction's buffer.
// If the transaction doesn't exist, it creates a new transaction with the event.
func (tc *InMemoryCache) AddEvent(txnID string, scn uint64, event *sqlredo.DMLEvent) {
	if txn, exists := tc.transactions[txnID]; exists {
		txn.Events = append(txn.Events, event)
		// tc.eventsMetric.Incr(1)
	} else {
		// Transaction not started yet, create it. This is an edgecase that _shouldn't_ happen.
		tc.log.Warnf("Transaction %s not found for event", txnID)
		tc.StartTransaction(txnID, scn)
	}
}

// GetTransaction retrieves the transaction with the given ID from the cache.
// Returns nil if the transaction doesn't exist.
func (tc *InMemoryCache) GetTransaction(txnID string) *Transaction {
	return tc.transactions[txnID]
}

// CommitTransaction removes the committed transaction from the cache.
func (tc *InMemoryCache) CommitTransaction(txnID string) {
	delete(tc.transactions, txnID)
	tc.transactionsMetric.Decr(1)
}

// RollbackTransaction removes the rolled back transaction from the cache, discarding all buffered events.
func (tc *InMemoryCache) RollbackTransaction(txnID string) {
	delete(tc.transactions, txnID)
	tc.transactionsMetric.Decr(1)
}
