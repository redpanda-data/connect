package types

import (
	"context"
	"net/http"
	"time"
)

//------------------------------------------------------------------------------

// Cache is a key/value store that can be shared across components and executing
// threads of a Benthos service.
type Cache interface {
	// Get attempts to locate and return a cached value by its key, returns an
	// error if the key does not exist or if the command fails.
	Get(key string) ([]byte, error)

	// Set attempts to set the value of a key, returns an error if the command
	// fails.
	Set(key string, value []byte) error

	// SetMulti attempts to set the value of multiple keys, returns an error if
	// any of the keys fail.
	SetMulti(items map[string][]byte) error

	// Add attempts to set the value of a key only if the key does not already
	// exist, returns an error if the key already exists or if the command
	// fails.
	Add(key string, value []byte) error

	// Delete attempts to remove a key. Returns an error if a failure occurs.
	Delete(key string) error

	Closable
}

// CacheTTLItem contains a value to cache along with an optional TTL.
type CacheTTLItem struct {
	Value []byte
	TTL   *time.Duration
}

// CacheWithTTL is a key/value store that can be shared across components and
// executing threads of a Benthos service.
type CacheWithTTL interface {
	// SetWithTTL attempts to set the value of a key, returns an error if the
	// command fails.
	SetWithTTL(key string, value []byte, ttl *time.Duration) error

	// SetMultiWithTTL attempts to set the value of multiple keys, returns an
	// error if any of the keys fail.
	SetMultiWithTTL(items map[string]CacheTTLItem) error

	// AddWithTTL attempts to set the value of a key only if the key does not
	// already exist, returns an error if the key already exists or if the
	// command fails.
	AddWithTTL(key string, value []byte, ttl *time.Duration) error

	Cache
}

//------------------------------------------------------------------------------

// RateLimit is a strategy for limiting access to a shared resource, this
// strategy can be safely used by components in parallel.
type RateLimit interface {
	// Access the rate limited resource. Returns a duration or an error if the
	// rate limit check fails. The returned duration is either zero (meaning the
	// resource may be accessed) or a reasonable length of time to wait before
	// requesting again.
	Access() (time.Duration, error)

	Closable
}

//------------------------------------------------------------------------------

// Condition reads a message, calculates a condition and returns a boolean.
type Condition interface {
	// Check tests a message against a configured condition.
	Check(msg Message) bool
}

//------------------------------------------------------------------------------

// Processor reads a message, performs some form of data processing to the
// message, and returns either a slice of >= 1 resulting messages or a response
// to return to the message origin.
type Processor interface {
	// ProcessMessage attempts to process a message. This method returns both a
	// slice of messages or a response indicating whether messages were dropped
	// due to an intermittent error (response.NewError) or were intentionally
	// filtered (response.NewAck).
	//
	// If an error occurs due to the contents of a message being invalid and you
	// wish to expose this as a recoverable fault you can use processor.FlagErr
	// to flag a message as having failed without dropping it.
	//
	// More information about this form of error handling can be found at:
	// https://www.benthos.dev/docs/configuration/error_handling
	ProcessMessage(Message) ([]Message, Response)

	Closable
}

//------------------------------------------------------------------------------

// Manager is an interface expected by Benthos components that allows them to
// register their service wide behaviours such as HTTP endpoints and event
// listeners, and obtain service wide shared resources such as caches.
type Manager interface {
	// RegisterEndpoint registers a server wide HTTP endpoint.
	RegisterEndpoint(path, desc string, h http.HandlerFunc)

	// GetInput attempts to find a service wide input by its name.
	// TODO: V4 Add this
	// GetInput(name string) (Input, error)

	// GetCache attempts to find a service wide cache by its name.
	GetCache(name string) (Cache, error)

	// GetCondition attempts to find a service wide condition by its name.
	GetCondition(name string) (Condition, error)

	// GetProcessor attempts to find a service wide processor by its name.
	// TODO: V4 Add this
	// GetProcessor(name string) (Processor, error)

	// GetRateLimit attempts to find a service wide rate limit by its name.
	GetRateLimit(name string) (RateLimit, error)

	// GetOutput attempts to find a service wide output by its name.
	// TODO: V4 Add this
	// GetOutput(name string) (OutputWriter, error)

	// GetPlugin attempts to find a service wide resource plugin by its name.
	GetPlugin(name string) (interface{}, error)

	// GetPipe attempts to find a service wide transaction chan by its name.
	GetPipe(name string) (<-chan Transaction, error)

	// SetPipe registers a transaction chan under a name.
	SetPipe(name string, t <-chan Transaction)

	// UnsetPipe removes a named transaction chan.
	UnsetPipe(name string, t <-chan Transaction)
}

//------------------------------------------------------------------------------

// Closable defines a type that can be safely closed down and cleaned up. This
// interface is required for many components within Benthos, but if your
// implementation is stateless and does not require shutting down then this
// interface can be implemented with empty shims.
type Closable interface {
	// CloseAsync triggers the shut down of this component but should not block
	// the calling goroutine.
	CloseAsync()

	// WaitForClose is a blocking call to wait until the component has finished
	// shutting down and cleaning up resources.
	WaitForClose(timeout time.Duration) error
}

//------------------------------------------------------------------------------

// Producer is a type that sends messages as transactions and waits for a
// response back, the response indicates whether the message was successfully
// propagated to a new destination (and can be discarded from the source.)
type Producer interface {
	// TransactionChan returns a channel used for consuming transactions from
	// this type. Every transaction received must be resolved before another
	// transaction will be sent.
	TransactionChan() <-chan Transaction
}

// Consumer is the higher level consumer type.
type Consumer interface {
	// Consume starts the type receiving transactions from a Transactor.
	Consume(<-chan Transaction) error
}

//------------------------------------------------------------------------------

// OutputWriter is a non-channel based output interface.
type OutputWriter interface {
	Closable

	// WriteTransaction attempts to write a transaction to an output.
	WriteTransaction(context.Context, Transaction) error

	// Connected returns a boolean indicating whether this output is currently
	// connected to its target.
	Connected() bool
}

// Output is a closable Consumer.
type Output interface {
	Consumer
	Closable

	// Connected returns a boolean indicating whether this output is currently
	// connected to its target.
	Connected() bool
}

// Input is a closable Producer.
type Input interface {
	Producer
	Closable

	// Connected returns a boolean indicating whether this input is currently
	// connected to its target.
	Connected() bool
}

// Pipeline is an interface that implements both the Consumer and Producer
// interfaces, and can therefore be used to pipe messages from Producer to a
// Consumer.
type Pipeline interface {
	Producer
	Consumer
	Closable
}

//------------------------------------------------------------------------------

// ProcessorConstructorFunc is a constructor to be called for each parallel
// stream pipeline thread in order to construct a custom processor
// implementation.
type ProcessorConstructorFunc func() (Processor, error)

// PipelineConstructorFunc is a constructor to be called for each parallel
// stream pipeline thread in order to construct a custom pipeline
// implementation.
//
// An integer pointer is provided to pipeline constructors that tracks the
// number of components spanning multiple pipelines. Each pipeline is expected
// to increment i by the number of components they contain, and may use the
// value for metric and logging namespacing.
type PipelineConstructorFunc func(i *int) (Pipeline, error)

//------------------------------------------------------------------------------

// Response is a response from an output, agent or broker that confirms the
// input of successful message receipt.
type Response interface {
	// Error returns a non-nil error if the message failed to reach its
	// destination.
	Error() error
}

//------------------------------------------------------------------------------
