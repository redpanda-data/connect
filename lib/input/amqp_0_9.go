package input

import (
	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/lib/input/reader"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/Jeffail/benthos/v3/lib/util/tls"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeAMQP09] = TypeSpec{
		constructor: NewAMQP09,
		Summary: `
Connects to an AMQP (0.91) queue. AMQP is a messaging protocol used by various
message brokers, including RabbitMQ.`,
		Description: `
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
- All existing message headers, including nested headers prefixed with the key of their respective parent.
` + "```" + `

You can access these metadata fields using
[function interpolation](/docs/configuration/interpolation#metadata).`,
		sanitiseConfigFunc: func(conf Config) (interface{}, error) {
			return sanitiseWithBatch(conf.AMQP09, conf.AMQP09.Batching)
		},
		FieldSpecs: docs.FieldSpecs{
			docs.FieldCommon("url",
				"A URL to connect to.",
				"amqp://localhost:5672/",
				"amqps://guest:guest@localhost:5672/",
			),
			docs.FieldCommon("queue", "An AMQP queue to consume from."),
			docs.FieldAdvanced("queue_declare", `
Allows you to passively declare the target queue. If the queue already exists
then the declaration passively verifies that they match the target fields.`,
				map[string]interface{}{
					"enabled": true,
					"durable": false,
				},
			),
			docs.FieldAdvanced("bindings_declare",
				"Allows you to passively declare bindings for the target queue.",
				[]interface{}{
					map[string]interface{}{
						"exchange": "foo",
						"key":      "bar",
					},
				},
			),
			docs.FieldCommon("consumer_tag", "A consumer tag."),
			docs.FieldCommon("prefetch_count", "The maximum number of pending messages to have consumed at a time."),
			docs.FieldAdvanced("prefetch_size", "The maximum amount of pending messages measured in bytes to have consumed at a time."),
			tls.FieldSpec(),
			docs.FieldDeprecated("batching"),
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
