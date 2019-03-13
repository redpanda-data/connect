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
	"github.com/Jeffail/benthos/lib/input/reader"
	"github.com/Jeffail/benthos/lib/log"
	"github.com/Jeffail/benthos/lib/metrics"
	"github.com/Jeffail/benthos/lib/types"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeAMQP] = TypeSpec{
		constructor: NewAMQP,
		description: `
Connects to an AMQP (0.91) queue. AMQP is a messaging protocol used by various
message brokers, including RabbitMQ.

The field ` + "`max_batch_count`" + ` specifies the maximum number of prefetched
messages to be batched together. When more than one message is batched they can
be split into individual messages with the ` + "`split`" + ` processor.

It's possible for this input type to declare the target queue by setting
` + "`queue_declare.enabled` to `true`" + `, if the queue already exists then
the declaration passively verifies that they match the target fields.

Similarly, it is possible to declare queue bindings by adding objects to the
` + "`bindings_declare`" + ` array. Binding declare objects take the form of:

` + "``` yaml" + `
{
  "exchange": "benthos-exchange",
  "key": "benthos-key"
}
` + "```" + `

TLS is automatic when connecting to an ` + "`amqps`" + ` URL, but custom
settings can be enabled in the ` + "`tls`" + ` section.

### Metadata

This input adds the following metadata fields to each message:

` + "``` text" + `
- amqp_content_type
- amqp_content_encoding
- amqp_delivery_mode
- amqp_priority
- amqp_correlation_id
- amqp_reply_to
- amqp_expiration
- amqp_message_id
- amqp_timestamp
- amqp_type
- amqp_user_id
- amqp_app_id
- amqp_consumer_tag
- amqp_delivery_tag
- amqp_redelivered
- amqp_exchange
- amqp_routing_key
- All existing message headers, including nested headers prefixed with the key
  of their respective parent.
` + "```" + `

You can access these metadata fields using
[function interpolation](../config_interpolation.md#metadata).`,
	}
}

//------------------------------------------------------------------------------

// NewAMQP creates a new AMQP input type.
func NewAMQP(conf Config, mgr types.Manager, log log.Modular, stats metrics.Type) (Type, error) {
	a, err := reader.NewAMQP(conf.AMQP, log, stats)
	if err != nil {
		return nil, err
	}
	return NewReader("amqp", a, log, stats)
}

//------------------------------------------------------------------------------
