package logminer

import "time"

// Transaction buffers events until commit
type Transaction struct {
	ID     string
	SCN    int64
	Events []*DMLEvent
}
type TransactionCache struct {
	transactions map[string]*Transaction
}

func NewTransactionCache() *TransactionCache {
	return &TransactionCache{
		transactions: make(map[string]*Transaction),
	}
}

func (tc *TransactionCache) StartTransaction(txnID string, scn int64) {
	tc.transactions[txnID] = &Transaction{
		ID:     txnID,
		SCN:    scn,
		Events: []*DMLEvent{},
	}
}

func (tc *TransactionCache) AddEvent(txnID string, event *DMLEvent) {
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

func (tc *TransactionCache) GetTransaction(txnID string) *Transaction {
	return tc.transactions[txnID]
}

func (tc *TransactionCache) CommitTransaction(txnID string) {
	delete(tc.transactions, txnID)
}

func (tc *TransactionCache) RollbackTransaction(txnID string) {
	delete(tc.transactions, txnID)
}

// DMLEvent represents a parsed DML operation
type DMLEvent struct {
	Operation Operation
	Schema    string
	Table     string
	OldValues map[string]interface{}
	NewValues map[string]interface{}
	SQLRedo   string
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
