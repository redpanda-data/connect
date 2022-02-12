package output

import (
	"github.com/Jeffail/benthos/v3/internal/component/output"
	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/internal/impl/redis/old"
	"github.com/Jeffail/benthos/v3/internal/interop"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message/batch"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/output/writer"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeRedisList] = TypeSpec{
		constructor: fromSimpleConstructor(NewRedisList),
		Summary: `
Pushes messages onto the end of a Redis list (which is created if it doesn't
already exist) using the RPUSH command.`,
		Description: `
The field ` + "`key`" + ` supports
[interpolation functions](/docs/configuration/interpolation#bloblang-queries), allowing
you to create a unique key for each message.`,
		Async:   true,
		Batches: true,
		FieldSpecs: old.ConfigDocs().Add(
			docs.FieldCommon(
				"key", "The key for each message, function interpolations can be optionally used to create a unique key per message.",
				"benthos_list", "${!meta(\"kafka_key\")}", "${!json(\"doc.id\")}", "${!count(\"msgs\")}",
			).IsInterpolated(),
			docs.FieldCommon("max_in_flight", "The maximum number of messages to have in flight at a given time. Increase this to improve throughput."),
			batch.FieldSpec(),
		),
		Categories: []Category{
			CategoryServices,
		},
	}
}

//------------------------------------------------------------------------------

// NewRedisList creates a new RedisList output type.
func NewRedisList(conf Config, mgr interop.Manager, log log.Modular, stats metrics.Type) (output.Streamed, error) {
	w, err := writer.NewRedisListV2(conf.RedisList, mgr, log, stats)
	if err != nil {
		return nil, err
	}
	a, err := NewAsyncWriter(TypeRedisList, conf.RedisList.MaxInFlight, w, log, stats)
	if err != nil {
		return nil, err
	}
	return NewBatcherFromConfig(conf.RedisList.Batching, a, mgr, log, stats)
}

//------------------------------------------------------------------------------
