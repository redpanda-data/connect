package input

import (
	"time"

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

// CloseAsync triggers a closure of this object but does not block.
func (i *WithPipeline) CloseAsync() {
	i.in.CloseAsync()
	i.pipe.CloseAsync()
}

// WaitForClose is a blocking call to wait until the object has finished closing
// down and cleaning up resources.
func (i *WithPipeline) WaitForClose(timeout time.Duration) error {
	return i.pipe.WaitForClose(timeout)
}
