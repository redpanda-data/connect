package output

import (
	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/internal/impl/redis/old"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/output/writer"
	"github.com/Jeffail/benthos/v3/lib/types"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeRedisHash] = TypeSpec{
		constructor: fromSimpleConstructor(NewRedisHash),
		Summary: `
Sets Redis hash objects using the HMSET command.`,
		Description: `
The field ` + "`key`" + ` supports
[interpolation functions](/docs/configuration/interpolation#bloblang-queries), allowing
you to create a unique key for each message.

The field ` + "`fields`" + ` allows you to specify an explicit map of field
names to interpolated values, also evaluated per message of a batch:

` + "```yaml" + `
output:
  redis_hash:
    url: tcp://localhost:6379
    key: ${!json("id")}
    fields:
      topic: ${!meta("kafka_topic")}
      partition: ${!meta("kafka_partition")}
      content: ${!json("document.text")}
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
		FieldSpecs: old.ConfigDocs().Add(
			docs.FieldCommon(
				"key", "The key for each message, function interpolations should be used to create a unique key per message.",
				"${!meta(\"kafka_key\")}", "${!json(\"doc.id\")}", "${!count(\"msgs\")}",
			).IsInterpolated(),
			docs.FieldCommon("walk_metadata", "Whether all metadata fields of messages should be walked and added to the list of hash fields to set."),
			docs.FieldCommon("walk_json_object", "Whether to walk each message as a JSON object and add each key/value pair to the list of hash fields to set."),
			docs.FieldString("fields", "A map of key/value pairs to set as hash fields.").IsInterpolated().Map(),
			docs.FieldCommon("max_in_flight", "The maximum number of messages to have in flight at a given time. Increase this to improve throughput."),
		),
		Categories: []Category{
			CategoryServices,
		},
	}
}

//------------------------------------------------------------------------------

// NewRedisHash creates a new RedisHash output type.
func NewRedisHash(conf Config, mgr types.Manager, log log.Modular, stats metrics.Type) (Type, error) {
	rhash, err := writer.NewRedisHashV2(conf.RedisHash, mgr, log, stats)
	if err != nil {
		return nil, err
	}
	a, err := NewAsyncWriter(
		TypeRedisHash, conf.RedisHash.MaxInFlight, rhash, log, stats,
	)
	if err != nil {
		return nil, err
	}
	return OnlySinglePayloads(a), nil
}

//------------------------------------------------------------------------------
