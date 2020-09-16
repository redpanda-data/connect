package output

import (
	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message/batch"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/output/writer"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/Jeffail/benthos/v3/lib/util/aws/session"
	"github.com/Jeffail/benthos/v3/lib/util/retries"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeKinesis] = TypeSpec{
		constructor: NewKinesis,
		Summary: `
Sends messages to a Kinesis stream.`,
		Description: `
Both the ` + "`partition_key`" + `(required) and ` + "`hash_key`" + ` (optional)
fields can be dynamically set using function interpolations described
[here](/docs/configuration/interpolation#bloblang-queries). When sending batched messages the
interpolations are performed per message part.

### Credentials

By default Benthos will use a shared credentials file when connecting to AWS
services. It's also possible to set them explicitly at the component level,
allowing you to transfer data across accounts. You can find out more
[in this document](/docs/guides/aws).`,
		sanitiseConfigFunc: func(conf Config) (interface{}, error) {
			return sanitiseWithBatch(conf.Kinesis, conf.Kinesis.Batching)
		},
		Async:   true,
		Batches: true,
		FieldSpecs: docs.FieldSpecs{
			docs.FieldCommon("stream", "The stream to publish messages to."),
			docs.FieldCommon("partition_key", "A required key for partitioning messages.").SupportsInterpolation(false),
			docs.FieldAdvanced("hash_key", "A optional hash key for partitioning messages.").SupportsInterpolation(false),
			docs.FieldCommon("max_in_flight", "The maximum number of messages to have in flight at a given time. Increase this to improve throughput."),
			batch.FieldSpec(),
		}.Merge(session.FieldSpecs()).Merge(retries.FieldSpecs()),
		Categories: []Category{
			CategoryServices,
			CategoryAWS,
		},
	}
}

//------------------------------------------------------------------------------

// NewKinesis creates a new Kinesis output type.
func NewKinesis(conf Config, mgr types.Manager, log log.Modular, stats metrics.Type) (Type, error) {
	kin, err := writer.NewKinesis(conf.Kinesis, log, stats)
	if err != nil {
		return nil, err
	}
	var w Type
	if conf.Kinesis.MaxInFlight == 1 {
		w, err = NewWriter(
			TypeKinesis, kin, log, stats,
		)
	} else {
		w, err = NewAsyncWriter(
			TypeKinesis, conf.Kinesis.MaxInFlight, kin, log, stats,
		)
	}
	if err != nil {
		return w, err
	}
	return newBatcherFromConf(conf.Kinesis.Batching, w, mgr, log, stats)
}

//------------------------------------------------------------------------------
