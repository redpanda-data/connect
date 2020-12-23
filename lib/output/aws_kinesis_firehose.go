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
	Constructors[TypeAWSKinesisFirehose] = TypeSpec{
		constructor: NewAWSKinesisFirehose,
		Version:     "3.36.0",
		Summary: `
Sends messages to a Kinesis Firehose delivery stream.`,
		Description: `
### Credentials

By default Benthos will use a shared credentials file when connecting to AWS
services. It's also possible to set them explicitly at the component level,
allowing you to transfer data across accounts. You can find out more
[in this document](/docs/guides/aws).`,
		sanitiseConfigFunc: func(conf Config) (interface{}, error) {
			return sanitiseWithBatch(conf.AWSKinesisFirehose, conf.AWSKinesisFirehose.Batching)
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

	Constructors[TypeKinesisFirehose] = TypeSpec{
		constructor: NewKinesisFirehose,
		Status:      docs.StatusDeprecated,
		Summary: `
Sends messages to a Kinesis Firehose delivery stream.`,
		Description: `
## Alternatives

This output has been renamed to ` + "[`aws_kinesis_firehose`](/docs/components/outputs/aws_kinesis_firehose)" + `.

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

// NewAWSKinesisFirehose creates a new KinesisFirehose output type.
func NewAWSKinesisFirehose(conf Config, mgr types.Manager, log log.Modular, stats metrics.Type) (Type, error) {
	return newKinesisFirehose(TypeAWSKinesisFirehose, conf.AWSKinesisFirehose, mgr, log, stats)
}

// NewKinesisFirehose creates a new KinesisFirehose output type.
func NewKinesisFirehose(conf Config, mgr types.Manager, log log.Modular, stats metrics.Type) (Type, error) {
	return newKinesisFirehose(TypeKinesisFirehose, conf.KinesisFirehose, mgr, log, stats)
}

func newKinesisFirehose(name string, conf writer.KinesisFirehoseConfig, mgr types.Manager, log log.Modular, stats metrics.Type) (Type, error) {
	kin, err := writer.NewKinesisFirehose(conf, log, stats)
	if err != nil {
		return nil, err
	}
	var w Type
	if conf.MaxInFlight == 1 {
		w, err = NewWriter(name, kin, log, stats)
	} else {
		w, err = NewAsyncWriter(name, conf.MaxInFlight, kin, log, stats)
	}
	if err != nil {
		return w, err
	}
	return newBatcherFromConf(conf.Batching, w, mgr, log, stats)
}

//------------------------------------------------------------------------------
