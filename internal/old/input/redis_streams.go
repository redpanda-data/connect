package input

import (
	"github.com/benthosdev/benthos/v4/internal/component/input"
	"github.com/benthosdev/benthos/v4/internal/component/metrics"
	"github.com/benthosdev/benthos/v4/internal/docs"
	"github.com/benthosdev/benthos/v4/internal/impl/redis/old"
	"github.com/benthosdev/benthos/v4/internal/interop"
	"github.com/benthosdev/benthos/v4/internal/log"
	"github.com/benthosdev/benthos/v4/internal/old/input/reader"
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
		Config: docs.FieldComponent().WithChildren(old.ConfigDocs()...).WithChildren(
			docs.FieldString("body_key", "The field key to extract the raw message from. All other keys will be stored in the message as metadata."),
			docs.FieldString("streams", "A list of streams to consume from.").Array(),
			docs.FieldInt("limit", "The maximum number of messages to consume from a single request."),
			docs.FieldString("client_id", "An identifier for the client connection."),
			docs.FieldString("consumer_group", "An identifier for the consumer group of the stream."),
			docs.FieldBool("create_streams", "Create subscribed streams if they do not exist (MKSTREAM option).").Advanced(),
			docs.FieldBool("start_from_oldest", "If an offset is not found for a stream, determines whether to consume from the oldest available offset, otherwise messages are consumed from the latest offset.").Advanced(),
			docs.FieldString("commit_period", "The period of time between each commit of the current offset. Offsets are always committed during shutdown.").Advanced(),
			docs.FieldString("timeout", "The length of time to poll for new messages before reattempting.").Advanced(),
		),
		Categories: []string{
			"Services",
		},
	}
}

//------------------------------------------------------------------------------

// NewRedisStreams creates a new Redis List input type.
func NewRedisStreams(conf Config, mgr interop.Manager, log log.Modular, stats metrics.Type) (input.Streamed, error) {
	var c reader.Async
	var err error
	if c, err = reader.NewRedisStreams(conf.RedisStreams, log, stats); err != nil {
		return nil, err
	}
	c = reader.NewAsyncPreserver(c)
	return NewAsyncReader(TypeRedisStreams, true, c, log, stats)
}

//------------------------------------------------------------------------------
