package output

import (
	"time"

	"github.com/Jeffail/benthos/v3/internal/component/output"
	"github.com/Jeffail/benthos/v3/internal/shutdown"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/types"
)

//------------------------------------------------------------------------------

// WithPipeline is a type that wraps both an output type and a pipeline type
// by routing the pipeline through the output, and implements the output.Type
// interface in order to act like an ordinary output.
type WithPipeline struct {
	out  Type
	pipe types.Pipeline
}

// WrapWithPipeline routes a processing pipeline directly into an output and
// returns a type that manages both and acts like an ordinary output.
func WrapWithPipeline(i *int, out Type, pipeConstructor types.PipelineConstructorFunc) (*WithPipeline, error) {
	pipe, err := pipeConstructor(i)
	if err != nil {
		return nil, err
	}

	if err := out.Consume(pipe.TransactionChan()); err != nil {
		return nil, err
	}
	return &WithPipeline{
		out:  out,
		pipe: pipe,
	}, nil
}

// WrapWithPipelines wraps an output with a variadic number of pipelines.
func WrapWithPipelines(out Type, pipeConstructors ...types.PipelineConstructorFunc) (Type, error) {
	procs := 0
	var err error
	for i := len(pipeConstructors) - 1; i >= 0; i-- {
		if out, err = WrapWithPipeline(&procs, out, pipeConstructors[i]); err != nil {
			return nil, err
		}
	}
	return out, nil
}

//------------------------------------------------------------------------------

// Consume starts the type listening to a message channel from a
// producer.
func (i *WithPipeline) Consume(tsChan <-chan message.Transaction) error {
	return i.pipe.Consume(tsChan)
}

// MaxInFlight returns the maximum number of in flight messages permitted by the
// output. This value can be used to determine a sensible value for parent
// outputs, but should not be relied upon as part of dispatcher logic.
func (i *WithPipeline) MaxInFlight() (int, bool) {
	return output.GetMaxInFlight(i.out)
}

// Connected returns a boolean indicating whether this output is currently
// connected to its target.
func (i *WithPipeline) Connected() bool {
	return i.out.Connected()
}

//------------------------------------------------------------------------------

// CloseAsync triggers a closure of this object but does not block.
func (i *WithPipeline) CloseAsync() {
	i.pipe.CloseAsync()
	go func() {
		_ = i.pipe.WaitForClose(shutdown.MaximumShutdownWait())
		i.out.CloseAsync()
	}()
}

// WaitForClose is a blocking call to wait until the object has finished closing
// down and cleaning up resources.
func (i *WithPipeline) WaitForClose(timeout time.Duration) error {
	return i.out.WaitForClose(timeout)
}

//------------------------------------------------------------------------------
