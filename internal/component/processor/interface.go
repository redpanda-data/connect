package processor

import (
	"time"

	"github.com/Jeffail/benthos/v3/lib/message"
)

// V1 is a common interface implemented by processors.
type V1 interface {
	// ProcessMessage attempts to process a message. This method returns both a
	// slice of messages or a response indicating whether messages were dropped
	// due to an intermittent error or were intentionally filtered.
	//
	// If an error occurs due to the contents of a message being invalid and you
	// wish to expose this as a recoverable fault you can use FlagErr to flag a
	// message as having failed without dropping it.
	//
	// More information about this form of error handling can be found at:
	// https://www.benthos.dev/docs/configuration/error_handling
	ProcessMessage(*message.Batch) ([]*message.Batch, error)

	// CloseAsync triggers the shut down of this component but should not block
	// the calling goroutine.
	CloseAsync()

	// WaitForClose is a blocking call to wait until the component has finished
	// shutting down and cleaning up resources.
	WaitForClose(timeout time.Duration) error
}

// Pipeline is an interface that implements channel based based consumer and
// producer methods for streaming data through a processing pipeline.
type Pipeline interface {
	// TransactionChan returns a channel used for consuming transactions from
	// this type. Every transaction received must be resolved before another
	// transaction will be sent.
	TransactionChan() <-chan message.Transaction

	// Consume starts the type receiving transactions from a Transactor.
	Consume(<-chan message.Transaction) error

	// CloseAsync triggers the shut down of this component but should not block
	// the calling goroutine.
	CloseAsync()

	// WaitForClose is a blocking call to wait until the component has finished
	// shutting down and cleaning up resources.
	WaitForClose(timeout time.Duration) error
}

// PipelineConstructorFunc is a constructor to be called for each parallel
// stream pipeline thread in order to construct a custom pipeline
// implementation.
//
// An integer pointer is provided to pipeline constructors that tracks the
// number of components spanning multiple pipelines. Each pipeline is expected
// to increment i by the number of components they contain, and may use the
// value for metric and logging namespacing.
type PipelineConstructorFunc func(i *int) (Pipeline, error)
