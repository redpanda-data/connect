package message

import "github.com/Jeffail/benthos/v3/lib/response"

// Transaction is a type respesenting a transaction containing a payload (the
// message) and a response channel, which is used to indicate whether the
// message was successfully propagated to the next destination.
type Transaction struct {
	// Payload is the message payload of this transaction.
	Payload *Batch

	// ResponseChan should receive a response at the end of a transaction (once
	// the message is no longer owned by the receiver.) The response itself
	// indicates whether the message has been propagated successfully.
	ResponseChan chan<- response.Error
}

//------------------------------------------------------------------------------

// NewTransaction creates a new transaction object from a message payload and a
// response channel.
func NewTransaction(payload *Batch, resChan chan<- response.Error) Transaction {
	return Transaction{
		Payload:      payload,
		ResponseChan: resChan,
	}
}
