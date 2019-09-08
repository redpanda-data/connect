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

package input

import (
	"time"

	"github.com/Jeffail/benthos/v3/lib/types"
)

//------------------------------------------------------------------------------

// WithPipeline is a type that wraps both an input type and a pipeline type
// by routing the input through the pipeline, and implements the input.Type
// interface in order to act like an ordinary input.
type WithPipeline struct {
	in   Type
	pipe types.Pipeline
}

// WrapWithPipeline routes an input directly into a processing pipeline and
// returns a type that manages both and acts like an ordinary input.
func WrapWithPipeline(procs *int, in Type, pipeConstructor types.PipelineConstructorFunc) (*WithPipeline, error) {
	pipe, err := pipeConstructor(procs)
	if err != nil {
		return nil, err
	}

	if err = pipe.Consume(in.TransactionChan()); err != nil {
		return nil, err
	}
	return &WithPipeline{
		in:   in,
		pipe: pipe,
	}, nil
}

// WrapWithPipelines wraps an input with a variadic number of pipelines.
func WrapWithPipelines(in Type, pipeConstructors ...types.PipelineConstructorFunc) (Type, error) {
	procs := 0
	var err error
	for _, ctor := range pipeConstructors {
		if in, err = WrapWithPipeline(&procs, in, ctor); err != nil {
			return nil, err
		}
	}
	return in, nil
}

//------------------------------------------------------------------------------

// TransactionChan returns the channel used for consuming transactions from this
// input.
func (i *WithPipeline) TransactionChan() <-chan types.Transaction {
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

//------------------------------------------------------------------------------
