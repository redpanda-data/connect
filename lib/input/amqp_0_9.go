package input

import (
	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/lib/input/reader"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/Jeffail/benthos/v3/lib/util/tls"
)

func init() {
	Constructors[TypeAMQP09] = TypeSpec{
		constructor: fromSimpleConstructor(NewAMQP09),
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
		Categories: []Category{
			CategoryServices,
		},
		FieldSpecs: docs.FieldSpecs{
			docs.FieldString("urls",
				"A list of URLs to connect to. The first URL to successfully establish a connection will be used until the connection is closed. If an item of the list contains commas it will be expanded into multiple URLs.",
				[]string{"amqp://guest:guest@127.0.0.1:5672/"},
				[]string{"amqp://127.0.0.1:5672/,amqp://127.0.0.2:5672/"},
				[]string{"amqp://127.0.0.1:5672/", "amqp://127.0.0.2:5672/"},
			).Array().AtVersion("3.58.0"),
			docs.FieldCommon("queue", "An AMQP queue to consume from."),
			docs.FieldAdvanced("queue_declare", `
Allows you to passively declare the target queue. If the queue already exists
then the declaration passively verifies that they match the target fields.`,
			).WithChildren(
				docs.FieldAdvanced("enabled", "Whether to enable queue declaration.").HasDefault(false),
				docs.FieldAdvanced("durable", "Whether the declared queue is durable.").HasDefault(false),
			),
			docs.FieldAdvanced("bindings_declare",
				"Allows you to passively declare bindings for the target queue.",
				[]interface{}{
					map[string]interface{}{
						"exchange": "foo",
						"key":      "bar",
					},
				},
			).Array().WithChildren(
				docs.FieldString("exchange", "The exchange of the declared binding.").HasDefault(""),
				docs.FieldString("key", "The key of the declared binding.").HasDefault(""),
			),
			docs.FieldCommon("consumer_tag", "A consumer tag."),
			docs.FieldAdvanced("auto_ack", "Acknowledge messages automatically as they are consumed rather than waiting for acknowledgments from downstream. This can improve throughput and prevent the pipeline from blocking but at the cost of eliminating delivery guarantees."),
			docs.FieldCommon("prefetch_count", "The maximum number of pending messages to have consumed at a time."),
			docs.FieldAdvanced("prefetch_size", "The maximum amount of pending messages measured in bytes to have consumed at a time."),
			tls.FieldSpec(),
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
	return NewAsyncReader(TypeAMQP09, true, a, log, stats)
}
