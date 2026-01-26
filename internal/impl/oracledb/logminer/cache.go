package logminer

import (
	"sync"
	"time"

	"github.com/redpanda-data/benthos/v4/public/service"
)

// TransactionCache is responsible for buffering transactions until a commit event is received,
// at which point we know it's safe to flush transactions to the Connect pipeline.
// If a rollback events is received the cache will be be cleared instead of flushed.
type TransactionCache interface {
	StartTransaction(txnID string, scn int64)
	AddEvent(txnID string, event *DMLEvent)
	GetTransaction(txnID string) *Transaction
	CommitTransaction(txnID string)
	RollbackTransaction(txnID string)
	Count()
}

type TransactionID string

// Transaction buffers events until commit
type Transaction struct {
	ID     string
	SCN    int64
	Events []*DMLEvent
}

type InMemoryCache struct {
	transactions map[string]*Transaction
	mu           sync.Mutex
	log          *service.Logger
}

func NewInMemoryCache(logger *service.Logger) *InMemoryCache {
	return &InMemoryCache{
		transactions: make(map[string]*Transaction),
		log:          logger,
	}
}

func (tc *InMemoryCache) StartTransaction(txnID string, scn int64) {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	tc.transactions[txnID] = &Transaction{
		ID:     txnID,
		SCN:    scn,
		Events: []*DMLEvent{},
	}
}

func (tc *InMemoryCache) AddEvent(txnID string, event *DMLEvent) {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	if txn, exists := tc.transactions[txnID]; exists {
		txn.Events = append(txn.Events, event)
	} else {
		// Transaction not started yet, create it
		tc.transactions[txnID] = &Transaction{
			ID:     txnID,
			Events: []*DMLEvent{event},
		}
	}
}

func (tc *InMemoryCache) GetTransaction(txnID string) *Transaction {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	return tc.transactions[txnID]
}

func (tc *InMemoryCache) CommitTransaction(txnID string) {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	delete(tc.transactions, txnID)
}

func (tc *InMemoryCache) RollbackTransaction(txnID string) {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	delete(tc.transactions, txnID)
}

func (tc *InMemoryCache) Count() {
	tc.mu.Lock()
	defer tc.mu.Unlock()

	for k, v := range tc.transactions {
		tc.log.Debugf("Cache: TransID %s: %d records", k, len(v.Events))
	}
}

// DMLEvent represents a parsed DML operation
type DMLEvent struct {
	Operation Operation
	Schema    string
	Table     string
	SQLRedo   string
	Data      map[string]any
	Timestamp time.Time
}

// Operation represents a LogMiner operation type
type Operation int

const (
	OpUnknown Operation = iota
	OpInsert
	OpDelete
	OpUpdate
	OpStart
	OpCommit
	OpRollback
)

func operationFromCode(code int) Operation {
	switch code {
	case 1:
		return OpInsert
	case 2:
		return OpDelete
	case 3:
		return OpUpdate
	case 6:
		return OpStart
	case 7:
		return OpCommit
	case 36:
		return OpRollback
	default:
		return OpUnknown
	}
}
