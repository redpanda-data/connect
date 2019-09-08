// Copyright (c) 2017 Ashley Jeffs
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package output

import (
	"time"

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

	if err = out.Consume(pipe.TransactionChan()); err != nil {
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
func (i *WithPipeline) Consume(tsChan <-chan types.Transaction) error {
	return i.pipe.Consume(tsChan)
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
		i.pipe.WaitForClose(time.Second)
		i.out.CloseAsync()
	}()
}

// WaitForClose is a blocking call to wait until the object has finished closing
// down and cleaning up resources.
func (i *WithPipeline) WaitForClose(timeout time.Duration) error {
	return i.out.WaitForClose(timeout)
}

//------------------------------------------------------------------------------
