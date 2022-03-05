package tracer

// Noop is a no-operation implementation of a tracer.
type Noop struct{}

// Close does nothing.
func (n Noop) Close() error {
	return nil
}
