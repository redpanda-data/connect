package input

import (
	"github.com/Jeffail/benthos/v3/lib/input/reader"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeAMQP09] = TypeSpec{
		constructor: NewAMQP09,
		Description: `
Connects to an AMQP (0.91) queue. AMQP is a messaging protocol used by various
message brokers, including RabbitMQ.

Messages consumed by this input can be processed in parallel, meaning a single
instance of this input can utilise any number of threads within a
` + "`pipeline`" + ` section of a config.

Use the ` + "`batching`" + ` fields to configure an optional
[batching policy](../batching.md#batch-policy).

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
		sanitiseConfigFunc: func(conf Config) (interface{}, error) {
			return sanitiseWithBatch(conf.AMQP09, conf.AMQP09.Batching)
		},
	}
}

//------------------------------------------------------------------------------

// NewAMQP09 creates a new AMQP09 input type.
func NewAMQP09(conf Config, mgr types.Manager, log log.Modular, stats metrics.Type) (Type, error) {
	var a reader.Async
	var err error
	if a, err = reader.NewAMQP09(conf.AMQP09, log, stats); err != nil {
		return nil, err
	}
	if a, err = reader.NewAsyncBatcher(conf.AMQP09.Batching, a, mgr, log, stats); err != nil {
		return nil, err
	}
	a = reader.NewAsyncBundleUnacks(a)
	return NewAsyncReader(TypeAMQP09, true, a, log, stats)
}

//------------------------------------------------------------------------------
