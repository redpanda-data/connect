package parallel

//------------------------------------------------------------------------------

// AckFunc is a func returned when a message is read from a parallel buffer. The
// func should be called when the message is finished with, and a flag indicates
// whether the message was successfully propagated and can be removed from the
// buffer. Returns the current backlog of the buffer in bytes, or an error if
// the message was not successfully removed.
//
// If an error is returned it is safe to call the function again. Otherwise, it
// is not.
//
// It is safe to call this func even if the buffer has closed.
type AckFunc func(ack bool) (int, error)

//------------------------------------------------------------------------------
