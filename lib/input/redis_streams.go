package input

import (
	"github.com/Jeffail/benthos/v3/lib/input/reader"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeRedisStreams] = TypeSpec{
		constructor: NewRedisStreams,
		Description: `
Pulls messages from Redis (v5.0+) streams with the XREADGROUP command. The
` + "`client_id`" + ` should be unique for each consumer of a group.

The field ` + "`limit`" + ` specifies the maximum number of records to be
received per request. When more than one record is returned they are batched and
can be split into individual messages with the ` + "`split`" + ` processor.

Messages consumed by this input can be processed in parallel, meaning a single
instance of this input can utilise any number of threads within a
` + "`pipeline`" + ` section of a config.

Use the ` + "`batching`" + ` fields to configure an optional
[batching policy](../batching.md#batch-policy).

Redis stream entries are key/value pairs, as such it is necessary to specify the
key that contains the body of the message. All other keys/value pairs are saved
as metadata fields.`,
		sanitiseConfigFunc: func(conf Config) (interface{}, error) {
			return sanitiseWithBatch(conf.RedisStreams, conf.RedisStreams.Batching)
		},
	}
}

//------------------------------------------------------------------------------

// NewRedisStreams creates a new Redis List input type.
func NewRedisStreams(conf Config, mgr types.Manager, log log.Modular, stats metrics.Type) (Type, error) {
	var c reader.Async
	var err error
	if c, err = reader.NewRedisStreams(conf.RedisStreams, log, stats); err != nil {
		return nil, err
	}
	if c, err = reader.NewAsyncBatcher(conf.RedisStreams.Batching, c, mgr, log, stats); err != nil {
		return nil, err
	}
	c = reader.NewAsyncBundleUnacks(reader.NewAsyncPreserver(c))
	return NewAsyncReader(TypeRedisStreams, true, c, log, stats)
}

//------------------------------------------------------------------------------
