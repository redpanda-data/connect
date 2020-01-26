package output

import (
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/output/writer"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/Jeffail/benthos/v3/lib/x/docs"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeRedisHash] = TypeSpec{
		constructor: NewRedisHash,
		Summary: `
Sets Redis hash objects using the HMSET command.`,
		Description: `
The field ` + "`key`" + ` supports
[interpolation functions](/docs/configuration/interpolation#functions), allowing
you to create a unique key for each message.

The field ` + "`fields`" + ` allows you to specify an explicit map of field
names to interpolated values, also evaluated per message of a batch:

` + "```yaml" + `
redis_hash:
  url: tcp://localhost:6379
  key: ${!json_field:id}
  fields:
    topic: ${!metadata:kafka_topic}
    partition: ${!metadata:kafka_partition}
    content: ${!json_field:document.text}
` + "```" + `

If the field ` + "`walk_metadata`" + ` is set to ` + "`true`" + ` then Benthos
will walk all metadata fields of messages and add them to the list of hash
fields to set.

If the field ` + "`walk_json_object`" + ` is set to ` + "`true`" + ` then
Benthos will walk each message as a JSON object, extracting keys and the string
representation of their value and adds them to the list of hash fields to set.

The order of hash field extraction is as follows:

1. Metadata (if enabled)
2. JSON object (if enabled)
3. Explicit fields

Where latter stages will overwrite matching field names of a former stage.`,
		Async: true,
		FieldSpecs: docs.FieldSpecs{
			docs.FieldCommon("url", "The URL of a Redis server to connect to.", "tcp://localhost:6379"),
			docs.FieldCommon(
				"key", "The key for each message, function interpolations should be used to create a unique key per message.",
				"${!metadata:kafka_key}", "${!json_field:doc.id}", "${!count:msgs}",
			).SupportsInterpolation(false),
			docs.FieldCommon("walk_metadata", "Whether all metadata fields of messages should be walked and added to the list of hash fields to set."),
			docs.FieldCommon("walk_json_object", "Whether to walk each message as a JSON object and add each key/value pair to the list of hash fields to set."),
			docs.FieldCommon("fields", "A map of key/value pairs to set as hash fields.").SupportsInterpolation(false),
			docs.FieldCommon("max_in_flight", "The maximum number of messages to have in flight at a given time. Increase this to improve throughput."),
		},
	}
}

//------------------------------------------------------------------------------

// NewRedisHash creates a new RedisHash output type.
func NewRedisHash(conf Config, mgr types.Manager, log log.Modular, stats metrics.Type) (Type, error) {
	rhash, err := writer.NewRedisHash(conf.RedisHash, log, stats)
	if err != nil {
		return nil, err
	}
	if conf.RedisHash.MaxInFlight == 1 {
		return NewWriter(
			TypeRedisHash, rhash, log, stats,
		)
	}
	return NewAsyncWriter(
		TypeRedisHash, conf.RedisHash.MaxInFlight, rhash, log, stats,
	)
}

//------------------------------------------------------------------------------
