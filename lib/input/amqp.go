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
	Constructors[TypeAMQP] = TypeSpec{
		constructor: fromSimpleConstructor(NewAMQP),
		Description: `
DEPRECATED: This input is deprecated and scheduled for removal in Benthos V4.
Please use [` + "`amqp_0_9`" + `](amqp_0_9) instead.`,
		Status: docs.StatusDeprecated,
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
				docs.FieldAdvanced("exchange", "The exchange of the declared binding.").HasDefault(""),
				docs.FieldAdvanced("key", "The key of the declared binding.").HasDefault(""),
			),
			docs.FieldDeprecated("max_batch_count"),
			docs.FieldCommon("consumer_tag", "A consumer tag."),
			docs.FieldCommon("prefetch_count", "The maximum number of pending messages to have consumed at a time."),
			docs.FieldAdvanced("prefetch_size", "The maximum amount of pending messages measured in bytes to have consumed at a time."),
			tls.FieldSpec(),
		},
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
