package output

import (
	"github.com/benthosdev/benthos/v4/internal/batch/policy"
	"github.com/benthosdev/benthos/v4/internal/component/metrics"
	"github.com/benthosdev/benthos/v4/internal/component/output"
	"github.com/benthosdev/benthos/v4/internal/docs"
	"github.com/benthosdev/benthos/v4/internal/impl/redis/old"
	"github.com/benthosdev/benthos/v4/internal/interop"
	"github.com/benthosdev/benthos/v4/internal/log"
	"github.com/benthosdev/benthos/v4/internal/metadata"
	"github.com/benthosdev/benthos/v4/internal/old/output/writer"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeRedisStreams] = TypeSpec{
		constructor: fromSimpleConstructor(NewRedisStreams),
		Summary: `
Pushes messages to a Redis (v5.0+) Stream (which is created if it doesn't
already exist) using the XADD command.`,
		Description: `
It's possible to specify a maximum length of the target stream by setting it to
a value greater than 0, in which case this cap is applied only when Redis is
able to remove a whole macro node, for efficiency.

Redis stream entries are key/value pairs, as such it is necessary to specify the
key to be set to the body of the message. All metadata fields of the message
will also be set as key/value pairs, if there is a key collision between
a metadata item and the body then the body takes precedence.`,
		Async:   true,
		Batches: true,
		FieldSpecs: old.ConfigDocs().Add(
			docs.FieldCommon("stream", "The stream to add messages to."),
			docs.FieldCommon("body_key", "A key to set the raw body of the message to."),
			docs.FieldCommon("max_length", "When greater than zero enforces a rough cap on the length of the target stream."),
			docs.FieldCommon("max_in_flight", "The maximum number of messages to have in flight at a given time. Increase this to improve throughput."),
			docs.FieldCommon("metadata", "Specify criteria for which metadata values are included in the message body.").WithChildren(metadata.ExcludeFilterFields()...),
			policy.FieldSpec(),
		),
		Categories: []Category{
			CategoryServices,
		},
	}
}

//------------------------------------------------------------------------------

// NewRedisStreams creates a new RedisStreams output type.
func NewRedisStreams(conf Config, mgr interop.Manager, log log.Modular, stats metrics.Type) (output.Streamed, error) {
	w, err := writer.NewRedisStreams(conf.RedisStreams, log, stats)
	if err != nil {
		return nil, err
	}
	a, err := NewAsyncWriter(TypeRedisStreams, conf.RedisStreams.MaxInFlight, w, log, stats)
	if err != nil {
		return nil, err
	}
	return NewBatcherFromConfig(conf.RedisStreams.Batching, a, mgr, log, stats)
}

//------------------------------------------------------------------------------
