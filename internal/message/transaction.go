package message

import "context"

// Transaction is a type respesenting a transaction containing a payload (the
// message) and a response channel, which is used to indicate whether the
// message was successfully propagated to the next destination.
type Transaction struct {
	// Payload is the message payload of this transaction.
	Payload *Batch

	// ResponseChan should receive a response at the end of a transaction (once
	// the message is no longer owned by the receiver.) The response itself
	// indicates whether the message has been propagated successfully.
	ResponseChan chan<- error
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

// Ack returns a delivery response back through the transaction to the message
// source. A nil error indicates that delivery has been completed successfully,
// a non-nil error indicates that the message could not be delivered and should
// be retried or nacked upstream.
func (t *Transaction) Ack(ctx context.Context, err error) error {
	select {
	case t.ResponseChan <- err:
	case <-ctx.Done():
		return ctx.Err()
	}
	return nil
}
