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
	Payload *Batch

	// ResponseChan should receive a response at the end of a transaction (once
	// the message is no longer owned by the receiver.) The response itself
	// indicates whether the message has been propagated successfully.
	ResponseChan chan<- error

	// responseFunc should be called with an error at the end of a transaction
	// (once the message is no longer owned by the receiver.) The error
	// indicates whether the message has been propagated successfully.
	responseFunc func(context.Context, error) error
}

//------------------------------------------------------------------------------

// NewTransaction creates a new transaction object from a message payload and a
// response channel.
func NewTransaction(payload *Batch, resChan chan<- error) Transaction {
	return Transaction{
		Payload:      payload,
		ResponseChan: resChan,
	}
}

// NewTransactionFunc creates a new transaction object that associates a message
// batch payload with a func used to acknowledge delivery of the message batch.
func NewTransactionFunc(payload *Batch, fn func(context.Context, error) error) Transaction {
	return Transaction{
		Payload:      payload,
		responseFunc: fn,
	}
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
	case t.ResponseChan <- err:
	case <-ctx.Done():
		return ctx.Err()
	}
	return nil
}
