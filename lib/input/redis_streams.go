package input

import (
	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/internal/impl/redis"
	"github.com/Jeffail/benthos/v3/lib/input/reader"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message/batch"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeRedisStreams] = TypeSpec{
		constructor: fromSimpleConstructor(NewRedisStreams),
		Summary: `
Pulls messages from Redis (v5.0+) streams with the XREADGROUP command. The
` + "`client_id`" + ` should be unique for each consumer of a group.`,
		Description: `
Redis stream entries are key/value pairs, as such it is necessary to specify the
key that contains the body of the message. All other keys/value pairs are saved
as metadata fields.`,
		FieldSpecs: redis.ConfigDocs().Add(
			func() docs.FieldSpec {
				b := batch.FieldSpec()
				b.IsDeprecated = true
				return b
			}(),
			docs.FieldCommon("body_key", "The field key to extract the raw message from. All other keys will be stored in the message as metadata."),
			docs.FieldCommon("streams", "A list of streams to consume from.").Array(),
			docs.FieldCommon("limit", "The maximum number of messages to consume from a single request."),
			docs.FieldCommon("client_id", "An identifier for the client connection."),
			docs.FieldCommon("consumer_group", "An identifier for the consumer group of the stream."),
			docs.FieldAdvanced("create_streams", "Create subscribed streams if they do not exist (MKSTREAM option)."),
			docs.FieldAdvanced("start_from_oldest", "If an offset is not found for a stream, determines whether to consume from the oldest available offset, otherwise messages are consumed from the latest offset."),
			docs.FieldAdvanced("commit_period", "The period of time between each commit of the current offset. Offsets are always committed during shutdown."),
			docs.FieldAdvanced("timeout", "The length of time to poll for new messages before reattempting."),
		),
		Categories: []Category{
			CategoryServices,
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
	c = reader.NewAsyncPreserver(c)
	return NewAsyncReader(TypeRedisStreams, true, c, log, stats)
}

//------------------------------------------------------------------------------
