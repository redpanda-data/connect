package response

import "errors"

// Errors for response types.
var (
	ErrNoAck = errors.New("message failed to reach a target destination")
)

//------------------------------------------------------------------------------

// Error is a response type that wraps an error, this error will be interpreted
// as a failed message signal if the error is non-nil, a nil error indicates the
// message has successfully ended up somewhere and can be acknowledged upstream.
type Error struct {
	err error
}

// AckError returns the underlying error.
func (o Error) AckError() error {
	return o.err
}

// NewError returns a response that wraps an error (nil error signals successful
// receipt).
func NewError(err error) Error {
	return Error{
		err: err,
	}
}

//------------------------------------------------------------------------------

// Ack is a response type that indicates the message has reached a destination
// and can be acknowledged upstream.
type Ack struct{}

// AckError returns nil.
func (a Ack) AckError() error { return nil }

// NewAck returns an Ack response type.
func NewAck() Ack {
	return Ack{}
}
