package input

import (
	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/internal/impl/nats/auth"
	"github.com/Jeffail/benthos/v3/lib/input/reader"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/Jeffail/benthos/v3/lib/util/tls"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeNATSStream] = TypeSpec{
		constructor: fromSimpleConstructor(NewNATSStream),
		Summary: `
Subscribe to a NATS Stream subject. Joining a queue is optional and allows
multiple clients of a subject to consume using queue semantics.`,
		Description: `
Tracking and persisting offsets through a durable name is also optional and
works with or without a queue. If a durable name is not provided then subjects
are consumed from the most recently published message.

When a consumer closes its connection it unsubscribes, when all consumers of a
durable queue do this the offsets are deleted. In order to avoid this you can
stop the consumers from unsubscribing by setting the field
` + "`unsubscribe_on_close` to `false`" + `.

### Metadata

This input adds the following metadata fields to each message:

` + "``` text" + `
- nats_stream_subject
- nats_stream_sequence
` + "```" + `

You can access these metadata fields using
[function interpolation](/docs/configuration/interpolation#metadata).

` + auth.Description(),
		FieldSpecs: docs.FieldSpecs{
			docs.FieldCommon(
				"urls",
				"A list of URLs to connect to. If an item of the list contains commas it will be expanded into multiple URLs.",
				[]string{"nats://127.0.0.1:4222"},
				[]string{"nats://username:password@127.0.0.1:4222"},
			).Array(),
			docs.FieldCommon("cluster_id", "The ID of the cluster to consume from."),
			docs.FieldCommon("client_id", "A client ID to connect as."),
			docs.FieldCommon("queue", "The queue to consume from."),
			docs.FieldCommon("subject", "A subject to consume from."),
			docs.FieldCommon("durable_name", "Preserve the state of your consumer under a durable name."),
			docs.FieldCommon("unsubscribe_on_close", "Whether the subscription should be destroyed when this client disconnects."),
			docs.FieldAdvanced("start_from_oldest", "If a position is not found for a queue, determines whether to consume from the oldest available message, otherwise messages are consumed from the latest."),
			docs.FieldAdvanced("max_inflight", "The maximum number of unprocessed messages to fetch at a given time."),
			docs.FieldAdvanced("ack_wait", "An optional duration to specify at which a message that is yet to be acked will be automatically retried."),
			tls.FieldSpec(),
			auth.FieldSpec(),
		},
		Categories: []Category{
			CategoryServices,
		},
	}
}

//------------------------------------------------------------------------------

// NewNATSStream creates a new NATSStream input type.
func NewNATSStream(conf Config, mgr types.Manager, log log.Modular, stats metrics.Type) (Type, error) {
	var c reader.Async
	var err error
	if c, err = reader.NewNATSStream(conf.NATSStream, log, stats); err != nil {
		return nil, err
	}
	return NewAsyncReader(TypeNATSStream, true, c, log, stats)
}

//------------------------------------------------------------------------------
