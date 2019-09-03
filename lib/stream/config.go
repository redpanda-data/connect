// Copyright (c) 2018 Ashley Jeffs
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

package stream

import (
	"github.com/Jeffail/benthos/v3/lib/buffer"
	"github.com/Jeffail/benthos/v3/lib/input"
	"github.com/Jeffail/benthos/v3/lib/output"
	"github.com/Jeffail/benthos/v3/lib/pipeline"
)

//------------------------------------------------------------------------------

// Config is a configuration struct representing all four layers of a Benthos
// stream.
type Config struct {
	Input    input.Config    `json:"input" yaml:"input"`
	Buffer   buffer.Config   `json:"buffer" yaml:"buffer"`
	Pipeline pipeline.Config `json:"pipeline" yaml:"pipeline"`
	Output   output.Config   `json:"output" yaml:"output"`
}

// NewConfig returns a new configuration with default values.
func NewConfig() Config {
	return Config{
		Input:    input.NewConfig(),
		Buffer:   buffer.NewConfig(),
		Pipeline: pipeline.NewConfig(),
		Output:   output.NewConfig(),
	}
}

// Sanitised returns a sanitised copy of the Benthos configuration, meaning
// fields of no consequence (unused inputs, outputs, processors etc) are
// excluded.
func (c Config) Sanitised() (interface{}, error) {
	inConf, err := input.SanitiseConfig(c.Input)
	if err != nil {
		return nil, err
	}

	var bufConf interface{}
	bufConf, err = buffer.SanitiseConfig(c.Buffer)
	if err != nil {
		return nil, err
	}

	var pipeConf interface{}
	pipeConf, err = pipeline.SanitiseConfig(c.Pipeline)
	if err != nil {
		return nil, err
	}

	var outConf interface{}
	outConf, err = output.SanitiseConfig(c.Output)
	if err != nil {
		return nil, err
	}

	return struct {
		Input    interface{} `json:"input" yaml:"input"`
		Buffer   interface{} `json:"buffer" yaml:"buffer"`
		Pipeline interface{} `json:"pipeline" yaml:"pipeline"`
		Output   interface{} `json:"output" yaml:"output"`
	}{
		Input:    inConf,
		Buffer:   bufConf,
		Pipeline: pipeConf,
		Output:   outConf,
	}, nil
}

//------------------------------------------------------------------------------
