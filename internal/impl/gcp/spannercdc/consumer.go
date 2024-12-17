package spannercdc

// Consumer is the interface to consume the DataChangeRecord.
//
// Consume might be called from multiple goroutines and must be re-entrant safe.
type Consumer interface {
	Consume(change *DataChangeRecord) error
}

// ConsumerFunc type is an adapter to allow the use of ordinary functions as Consumer.
type ConsumerFunc func(*DataChangeRecord) error

// Consume calls f(change).
func (f ConsumerFunc) Consume(change *DataChangeRecord) error {
	return f(change)
}
