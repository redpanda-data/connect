// Copyright (c) 2014 Ashley Jeffs
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

// +build ZMQ4

package input

import (
	"github.com/Jeffail/benthos/v3/lib/input/reader"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeZMQ4] = TypeSpec{
		constructor: NewZMQ4,
		description: `
ZMQ4 is supported but currently depends on C bindings. Since this is an
annoyance when building or using Benthos it is not compiled by default.

Build it into your project by getting libzmq installed on your machine, then
build with the tag: 'go install -tags "ZMQ4" github.com/Jeffail/benthos/v3/cmd/...'

ZMQ4 input supports PULL and SUB sockets only. If there is demand for other
socket types then they can be added easily.`,
	}
}

//------------------------------------------------------------------------------

// NewZMQ4 creates a new ZMQ input type.
func NewZMQ4(conf Config, mgr types.Manager, log log.Modular, stats metrics.Type) (Type, error) {
	z, err := reader.NewZMQ4(conf.ZMQ4, log, stats)
	if err != nil {
		return nil, err
	}
	return NewReader("zmq4", reader.NewPreserver(z), log, stats)
}

//------------------------------------------------------------------------------
