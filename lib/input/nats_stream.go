package input

import (
	"github.com/Jeffail/benthos/v3/lib/input/reader"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeNATSStream] = TypeSpec{
		constructor: NewNATSStream,
		Description: `
Subscribe to a NATS Stream subject, which is at-least-once. Joining a queue is
optional and allows multiple clients of a subject to consume using queue
semantics.

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
[function interpolation](../config_interpolation.md#metadata).`,
		sanitiseConfigFunc: func(conf Config) (interface{}, error) {
			return sanitiseWithBatch(conf.NATSStream, conf.NATSStream.Batching)
		},
		DeprecatedFields: []string{"batching"},
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
	c = reader.NewAsyncBundleUnacks(c)
	if c, err = reader.NewAsyncBatcher(conf.NATSStream.Batching, c, mgr, log, stats); err != nil {
		return nil, err
	}
	return NewAsyncReader(TypeNATSStream, true, c, log, stats)
}

//------------------------------------------------------------------------------
