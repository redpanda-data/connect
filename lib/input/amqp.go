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

package input

import (
	"github.com/Jeffail/benthos/v3/lib/input/reader"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeAMQP] = TypeSpec{
		constructor: NewAMQP,
		description: `
DEPRECATED: This input is deprecated and scheduled for removal in Benthos V4.
Please use [` + "`amqp_0_9`" + `](#amqp_0_9) instead.`,
	}
}

//------------------------------------------------------------------------------

// NewAMQP creates a new AMQP input type.
// TODO: V4 Remove this.
func NewAMQP(conf Config, mgr types.Manager, log log.Modular, stats metrics.Type) (Type, error) {
	log.Warnln("The amqp input is deprecated, please use amqp_0_9 instead.")
	a, err := reader.NewAMQP(conf.AMQP, log, stats)
	if err != nil {
		return nil, err
	}
	return NewReader("amqp", a, log, stats)
}

//------------------------------------------------------------------------------
