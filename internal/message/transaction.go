package message

import "context"

// Transaction is a component that associates a batch of one or more messages
// with a mechanism that is able to propagate an acknowledgement of delivery
// back to the source of the batch.
//
// This allows batches to be routed through complex component networks of
// buffers, processing pipelines and output brokers without losing the
// association.
//
// It would not be sufficient to associate acknowledgement to the message (or
// batch of messages) itself as it would then not be possible to expand and
// split message batches (grouping, etc) without loosening delivery guarantees.
//
// The proper way to do such things would be to create a new transaction for
// each resulting batch, and only when all derivative transactions are
// acknowledged is the source transaction acknowledged in turn.
type Transaction struct {
	// Payload is the message payload of this transaction.
	Payload Batch

	// responseChan should receive a response at the end of a transaction (once
	// the message is no longer owned by the receiver.) The response itself
	// indicates whether the message has been propagated successfully.
	responseChan chan<- error

	// responseFunc should be called with an error at the end of a transaction
	// (once the message is no longer owned by the receiver.) The error
	// indicates whether the message has been propagated successfully.
	responseFunc func(context.Context, error) error

	// Used for cancelling transactions. When cancelled it is up to the receiver
	// of this transaction to abort any attempt to deliver the transaction
	// message.
	ctx context.Context
}

//------------------------------------------------------------------------------

// NewTransaction creates a new transaction object from a message payload and a
// response channel.
func NewTransaction(payload Batch, resChan chan<- error) Transaction {
	return Transaction{
		Payload:      payload,
		responseChan: resChan,
		ctx:          context.Background(),
	}
}

// NewTransactionFunc creates a new transaction object that associates a message
// batch payload with a func used to acknowledge delivery of the message batch.
func NewTransactionFunc(payload Batch, fn func(context.Context, error) error) Transaction {
	return Transaction{
		Payload:      payload,
		responseFunc: fn,
		ctx:          context.Background(),
	}
}

// Context returns a context that indicates the cancellation of a transaction.
// It is optional for receivers of a transaction to honour this context, and is
// worth doing in cases where the transaction is blocked (on reconnect loops,
// etc) as it is often used as a fail-fast mechanism.
//
// When a transaction is aborted due to cancellation it is still required that
// acknowledgment is made, and should be done so with t.Context().Err().
func (t *Transaction) Context() context.Context {
	return t.ctx
}

// WithContext returns a copy of the transaction associated with a context used
// for cancellation. When cancelled it is up to the receiver of this transaction
// to abort any attempt to deliver the transaction message.
func (t *Transaction) WithContext(ctx context.Context) *Transaction {
	newT := *t
	newT.ctx = ctx
	return &newT
}

// Ack returns a delivery response back through the transaction to the message
// source. A nil error indicates that delivery has been completed successfully,
// a non-nil error indicates that the message could not be delivered and should
// be retried or nacked upstream.
func (t *Transaction) Ack(ctx context.Context, err error) error {
	if t.responseFunc != nil {
		return t.responseFunc(ctx, err)
	}
	select {
	case t.responseChan <- err:
	case <-ctx.Done():
		return ctx.Err()
	}
	return nil
}
