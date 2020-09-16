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
	Constructors[TypeKinesisFirehose] = TypeSpec{
		constructor: NewKinesisFirehose,
		Summary: `
Sends messages to a Kinesis Firehose delivery stream.`,
		Description: `
### Credentials

By default Benthos will use a shared credentials file when connecting to AWS
services. It's also possible to set them explicitly at the component level,
allowing you to transfer data across accounts. You can find out more
[in this document](/docs/guides/aws).`,
		sanitiseConfigFunc: func(conf Config) (interface{}, error) {
			return sanitiseWithBatch(conf.KinesisFirehose, conf.KinesisFirehose.Batching)
		},
		Async:   true,
		Batches: true,
		FieldSpecs: docs.FieldSpecs{
			docs.FieldCommon("stream", "The stream to publish messages to."),
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

// NewKinesisFirehose creates a new KinesisFirehose output type.
func NewKinesisFirehose(conf Config, mgr types.Manager, log log.Modular, stats metrics.Type) (Type, error) {
	kin, err := writer.NewKinesisFirehose(conf.KinesisFirehose, log, stats)
	if err != nil {
		return nil, err
	}
	var w Type
	if conf.KinesisFirehose.MaxInFlight == 1 {
		w, err = NewWriter(
			TypeKinesisFirehose, kin, log, stats,
		)
	} else {
		w, err = NewAsyncWriter(
			TypeKinesisFirehose, conf.KinesisFirehose.MaxInFlight, kin, log, stats,
		)
	}
	if err != nil {
		return w, err
	}
	return newBatcherFromConf(conf.KinesisFirehose.Batching, w, mgr, log, stats)
}

//------------------------------------------------------------------------------
