package output

import (
	"github.com/Jeffail/benthos/v3/internal/component/output"
	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/output/writer"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/Jeffail/benthos/v3/lib/util/tls"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeAMQP] = TypeSpec{
		constructor: fromSimpleConstructor(NewAMQP),
		Description: `
DEPRECATED: This output is deprecated and scheduled for removal in Benthos V4.
Please use [` + "`amqp_0_9`" + `](amqp_0_9) instead.`,
		Status: docs.StatusDeprecated,
		FieldSpecs: docs.FieldSpecs{
			docs.FieldCommon("url",
				"A URL to connect to.",
				"amqp://localhost:5672/",
				"amqps://guest:guest@localhost:5672/",
			),
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
			docs.FieldCommon("metadata", "Specify criteria for which metadata values are attached to objects as headers.").WithChildren(output.MetadataFields()...),
			docs.FieldCommon("max_in_flight", "The maximum number of messages to have in flight at a given time. Increase this to improve throughput."),
			docs.FieldAdvanced("persistent", "Whether message delivery should be persistent (transient by default)."),
			docs.FieldAdvanced("mandatory", "Whether to set the mandatory flag on published messages. When set if a published message is routed to zero queues it is returned."),
			docs.FieldAdvanced("immediate", "Whether to set the immediate flag on published messages. When set if there are no ready consumers of a queue then the message is dropped instead of waiting."),
			tls.FieldSpec(),
		},
	}
}

//------------------------------------------------------------------------------

// NewAMQP creates a new AMQP output type.
// TODO: V4 Remove this.
func NewAMQP(conf Config, mgr types.Manager, log log.Modular, stats metrics.Type) (Type, error) {
	log.Warnln("The amqp input is deprecated, please use amqp_0_9 instead.")
	a, err := writer.NewAMQP(conf.AMQP, log, stats)
	if err != nil {
		return nil, err
	}
	return NewWriter(
		"amqp", a, log, stats,
	)
}

//------------------------------------------------------------------------------
