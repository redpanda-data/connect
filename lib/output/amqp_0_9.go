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

package output

import (
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/output/writer"
	"github.com/Jeffail/benthos/v3/lib/types"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeAMQP09] = TypeSpec{
		constructor: NewAMQP09,
		description: `
Sends messages to an AMQP (0.91) exchange. AMQP is a messaging protocol used by
various message brokers, including RabbitMQ. The metadata from each message are
delivered as headers.

It's possible for this output type to create the target exchange by setting
` + "`exchange_declare.enabled` to `true`" + `, if the exchange already exists
then the declaration passively verifies that the settings match.

Exchange type options are: direct|fanout|topic|x-custom

TLS is automatic when connecting to an ` + "`amqps`" + ` URL, but custom
settings can be enabled in the ` + "`tls`" + ` section.

The field 'key' can be dynamically set using function interpolations described
[here](../config_interpolation.md#functions).`,
	}
}

//------------------------------------------------------------------------------

// NewAMQP09 creates a new AMQP output type.
func NewAMQP09(conf Config, mgr types.Manager, log log.Modular, stats metrics.Type) (Type, error) {
	a, err := writer.NewAMQP(conf.AMQP09, log, stats)
	if err != nil {
		return nil, err
	}
	return NewWriter(
		TypeAMQP09, a, log, stats,
	)
}

//------------------------------------------------------------------------------
