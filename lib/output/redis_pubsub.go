package output

import (
	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/internal/impl/redis/old"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message/batch"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/output/writer"
	"github.com/Jeffail/benthos/v3/lib/types"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeRedisPubSub] = TypeSpec{
		constructor: fromSimpleConstructor(NewRedisPubSub),
		Summary: `
Publishes messages through the Redis PubSub model. It is not possible to
guarantee that messages have been received.`,
		Description: `
This output will interpolate functions within the channel field, you
can find a list of functions [here](/docs/configuration/interpolation#bloblang-queries).`,
		Async:   true,
		Batches: true,
		FieldSpecs: old.ConfigDocs().Add(
			docs.FieldCommon("channel", "The channel to publish messages to.").IsInterpolated(),
			docs.FieldCommon("max_in_flight", "The maximum number of messages to have in flight at a given time. Increase this to improve throughput."),
			batch.FieldSpec(),
		),
		Categories: []Category{
			CategoryServices,
		},
	}
}

//------------------------------------------------------------------------------

// NewRedisPubSub creates a new RedisPubSub output type.
func NewRedisPubSub(conf Config, mgr types.Manager, log log.Modular, stats metrics.Type) (Type, error) {
	w, err := writer.NewRedisPubSubV2(conf.RedisPubSub, mgr, log, stats)
	if err != nil {
		return nil, err
	}
	a, err := NewAsyncWriter(TypeRedisPubSub, conf.RedisPubSub.MaxInFlight, w, log, stats)
	if err != nil {
		return nil, err
	}
	return NewBatcherFromConfig(conf.RedisPubSub.Batching, a, mgr, log, stats)
}

//------------------------------------------------------------------------------
