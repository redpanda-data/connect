package input

import (
	"context"

	iprocessor "github.com/benthosdev/benthos/v4/internal/component/processor"
	"github.com/benthosdev/benthos/v4/internal/message"
)

// WithPipeline is a type that wraps both an input type and a pipeline type
// by routing the input through the pipeline, and implements the input.Type
// interface in order to act like an ordinary input.
type WithPipeline struct {
	in   Streamed
	pipe iprocessor.Pipeline
}

// WrapWithPipeline routes an input directly into a processing pipeline and
// returns a type that manages both and acts like an ordinary input.
func WrapWithPipeline(in Streamed, pipeConstructor iprocessor.PipelineConstructorFunc) (*WithPipeline, error) {
	pipe, err := pipeConstructor()
	if err != nil {
		return nil, err
	}

	if err := pipe.Consume(in.TransactionChan()); err != nil {
		return nil, err
	}
	return &WithPipeline{
		in:   in,
		pipe: pipe,
	}, nil
}

// WrapWithPipelines wraps an input with a variadic number of pipelines.
func WrapWithPipelines(in Streamed, pipeConstructors ...iprocessor.PipelineConstructorFunc) (Streamed, error) {
	var err error
	for _, ctor := range pipeConstructors {
		if in, err = WrapWithPipeline(in, ctor); err != nil {
			return nil, err
		}
	}
	return in, nil
}

//------------------------------------------------------------------------------

// TransactionChan returns the channel used for consuming transactions from this
// input.
func (i *WithPipeline) TransactionChan() <-chan message.Transaction {
	return i.pipe.TransactionChan()
}

// Connected returns a boolean indicating whether this input is currently
// connected to its target.
func (i *WithPipeline) Connected() bool {
	return i.in.Connected()
}

//------------------------------------------------------------------------------

// TriggerStopConsuming instructs the input to start shutting down resources
// once all pending messages are delivered and acknowledged. This call does
// not block.
func (i *WithPipeline) TriggerStopConsuming() {
	i.in.TriggerStopConsuming()
}

// TriggerCloseNow triggers the shut down of this component but should not block
// the calling goroutine.
func (i *WithPipeline) TriggerCloseNow() {
	i.in.TriggerCloseNow()
	i.pipe.TriggerCloseNow()
}

// WaitForClose is a blocking call to wait until the component has finished
// shutting down and cleaning up resources.
func (i *WithPipeline) WaitForClose(ctx context.Context) error {
	return i.pipe.WaitForClose(ctx)
}
