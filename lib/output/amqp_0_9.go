package output

import (
	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/internal/metadata"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/output/writer"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/Jeffail/benthos/v3/lib/util/tls"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeAMQP09] = TypeSpec{
		constructor: fromSimpleConstructor(NewAMQP09),
		Summary: `
Sends messages to an AMQP (0.91) exchange. AMQP is a messaging protocol used by
various message brokers, including RabbitMQ.`,
		Description: `
The metadata from each message are delivered as headers.

It's possible for this output type to create the target exchange by setting
` + "`exchange_declare.enabled` to `true`" + `, if the exchange already exists
then the declaration passively verifies that the settings match.

TLS is automatic when connecting to an ` + "`amqps`" + ` URL, but custom
settings can be enabled in the ` + "`tls`" + ` section.

The fields 'key' and 'type' can be dynamically set using function interpolations described
[here](/docs/configuration/interpolation#bloblang-queries).`,
		Async: true,
		FieldSpecs: docs.FieldSpecs{
			docs.FieldString("urls",
				"A list of URLs to connect to. The first URL to successfully establish a connection will be used until the connection is closed. If an item of the list contains commas it will be expanded into multiple URLs.",
				[]string{"amqp://guest:guest@127.0.0.1:5672/"},
				[]string{"amqp://127.0.0.1:5672/,amqp://127.0.0.2:5672/"},
				[]string{"amqp://127.0.0.1:5672/", "amqp://127.0.0.2:5672/"},
			).Array().AtVersion("3.58.0"),
			docs.FieldCommon("exchange", "An AMQP exchange to publish to."),
			docs.FieldAdvanced("exchange_declare", "Optionally declare the target exchange (passive).").WithChildren(
				docs.FieldCommon("enabled", "Whether to declare the exchange."),
				docs.FieldCommon("type", "The type of the exchange.").HasOptions(
					"direct", "fanout", "topic", "x-custom",
				),
				docs.FieldCommon("durable", "Whether the exchange should be durable."),
			),
			docs.FieldCommon("key", "The binding key to set for each message.").IsInterpolated(),
			docs.FieldCommon("type", "The type property to set for each message.").IsInterpolated(),
			docs.FieldAdvanced("content_type", "The content type attribute to set for each message.").IsInterpolated(),
			docs.FieldAdvanced("content_encoding", "The content encoding attribute to set for each message.").IsInterpolated(),
			docs.FieldCommon("metadata", "Specify criteria for which metadata values are attached to messages as headers.").WithChildren(metadata.ExcludeFilterFields()...),
			docs.FieldAdvanced("priority", "Set the priority of each message with a dynamic interpolated expression.", "0", `${! meta("amqp_priority") }`, `${! json("doc.priority") }`).IsInterpolated(),
			docs.FieldCommon("max_in_flight", "The maximum number of messages to have in flight at a given time. Increase this to improve throughput."),
			docs.FieldAdvanced("persistent", "Whether message delivery should be persistent (transient by default)."),
			docs.FieldAdvanced("mandatory", "Whether to set the mandatory flag on published messages. When set if a published message is routed to zero queues it is returned."),
			docs.FieldAdvanced("immediate", "Whether to set the immediate flag on published messages. When set if there are no ready consumers of a queue then the message is dropped instead of waiting."),
			tls.FieldSpec(),
		},
		Categories: []Category{
			CategoryServices,
		},
	}
}

//------------------------------------------------------------------------------

// NewAMQP09 creates a new AMQP output type.
func NewAMQP09(conf Config, mgr types.Manager, log log.Modular, stats metrics.Type) (Type, error) {
	a, err := writer.NewAMQPV2(mgr, conf.AMQP09, log, stats)
	if err != nil {
		return nil, err
	}
	w, err := NewAsyncWriter(
		TypeAMQP09, conf.AMQP09.MaxInFlight, a, log, stats,
	)
	if err != nil {
		return nil, err
	}
	return OnlySinglePayloads(w), nil
}

//------------------------------------------------------------------------------
