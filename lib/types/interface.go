package types

import (
	"context"
	"net/http"
	"time"

	"github.com/Jeffail/benthos/v3/internal/component/cache"
	"github.com/Jeffail/benthos/v3/internal/component/processor"
	"github.com/Jeffail/benthos/v3/internal/component/ratelimit"
	"github.com/Jeffail/benthos/v3/lib/message"
)

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
	GetCache(name string) (cache.V1, error)

	// GetProcessor attempts to find a service wide processor by its name.
	// TODO: V4 Add this
	// GetProcessor(name string) (Processor, error)

	// GetRateLimit attempts to find a service wide rate limit by its name.
	GetRateLimit(name string) (ratelimit.V1, error)

	// GetOutput attempts to find a service wide output by its name.
	// TODO: V4 Add this
	// GetOutput(name string) (OutputWriter, error)

	// GetPipe attempts to find a service wide transaction chan by its name.
	GetPipe(name string) (<-chan message.Transaction, error)

	// SetPipe registers a transaction chan under a name.
	SetPipe(name string, t <-chan message.Transaction)

	// UnsetPipe removes a named transaction chan.
	UnsetPipe(name string, t <-chan message.Transaction)
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
	TransactionChan() <-chan message.Transaction
}

// Consumer is the higher level consumer type.
type Consumer interface {
	// Consume starts the type receiving transactions from a Transactor.
	Consume(<-chan message.Transaction) error
}

//------------------------------------------------------------------------------

// OutputWriter is a non-channel based output interface.
type OutputWriter interface {
	Closable

	// WriteTransaction attempts to write a transaction to an output.
	WriteTransaction(context.Context, message.Transaction) error

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
type ProcessorConstructorFunc func() (processor.V1, error)

// PipelineConstructorFunc is a constructor to be called for each parallel
// stream pipeline thread in order to construct a custom pipeline
// implementation.
//
// An integer pointer is provided to pipeline constructors that tracks the
// number of components spanning multiple pipelines. Each pipeline is expected
// to increment i by the number of components they contain, and may use the
// value for metric and logging namespacing.
type PipelineConstructorFunc func(i *int) (Pipeline, error)
