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
	Constructors[TypeAWSSQS] = TypeSpec{
		constructor: fromSimpleConstructor(NewAWSSQS),
		Version:     "3.36.0",
		Summary: `
Sends messages to an SQS queue.`,
		Description: `
Metadata values are sent along with the payload as attributes with the data type
String. If the number of metadata values in a message exceeds the message
attribute limit (10) then the top ten keys ordered alphabetically will be
selected.

The fields ` + "`message_group_id` and `message_deduplication_id`" + ` can be
set dynamically using
[function interpolations](/docs/configuration/interpolation#bloblang-queries), which are
resolved individually for each message of a batch.

### Credentials

By default Benthos will use a shared credentials file when connecting to AWS
services. It's also possible to set them explicitly at the component level,
allowing you to transfer data across accounts. You can find out more
[in this document](/docs/guides/aws).`,
		Async:   true,
		Batches: true,
		FieldSpecs: docs.FieldSpecs{
			docs.FieldCommon("url", "The URL of the target SQS queue."),
			docs.FieldCommon("message_group_id", "An optional group ID to set for messages.").IsInterpolated(),
			docs.FieldCommon("message_deduplication_id", "An optional deduplication ID to set for messages.").IsInterpolated(),
			docs.FieldCommon("max_in_flight", "The maximum number of messages to have in flight at a given time. Increase this to improve throughput."),
			batch.FieldSpec(),
		}.Merge(session.FieldSpecs()).Merge(retries.FieldSpecs()),
		Categories: []Category{
			CategoryServices,
			CategoryAWS,
		},
	}

	Constructors[TypeSQS] = TypeSpec{
		constructor: fromSimpleConstructor(NewAmazonSQS),
		Status:      docs.StatusDeprecated,
		Summary: `
Sends messages to an SQS queue.`,
		Description: `
## Alternatives

This output has been renamed to ` + "[`aws_sqs`](/docs/components/outputs/aws_sqs)" + `.

Metadata values are sent along with the payload as attributes with the data type
String. If the number of metadata values in a message exceeds the message
attribute limit (10) then the top ten keys ordered alphabetically will be
selected.

The fields ` + "`message_group_id` and `message_deduplication_id`" + ` can be
set dynamically using
[function interpolations](/docs/configuration/interpolation#bloblang-queries), which are
resolved individually for each message of a batch.

### Credentials

By default Benthos will use a shared credentials file when connecting to AWS
services. It's also possible to set them explicitly at the component level,
allowing you to transfer data across accounts. You can find out more
[in this document](/docs/guides/aws).`,
		Async:   true,
		Batches: true,
		FieldSpecs: docs.FieldSpecs{
			docs.FieldCommon("url", "The URL of the target SQS queue."),
			docs.FieldCommon("message_group_id", "An optional group ID to set for messages.").IsInterpolated(),
			docs.FieldCommon("message_deduplication_id", "An optional deduplication ID to set for messages.").IsInterpolated(),
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

// NewAWSSQS creates a new AmazonSQS output type.
func NewAWSSQS(conf Config, mgr types.Manager, log log.Modular, stats metrics.Type) (Type, error) {
	return newAmazonSQS(TypeAWSSQS, conf.AWSSQS, mgr, log, stats)
}

// NewAmazonSQS creates a new AmazonSQS output type.
func NewAmazonSQS(conf Config, mgr types.Manager, log log.Modular, stats metrics.Type) (Type, error) {
	return newAmazonSQS(TypeSQS, conf.SQS, mgr, log, stats)
}

func newAmazonSQS(name string, conf writer.AmazonSQSConfig, mgr types.Manager, log log.Modular, stats metrics.Type) (Type, error) {
	s, err := writer.NewAmazonSQS(conf, log, stats)
	if err != nil {
		return nil, err
	}
	var w Type
	if conf.MaxInFlight == 1 {
		w, err = NewWriter(name, s, log, stats)
	} else {
		w, err = NewAsyncWriter(name, conf.MaxInFlight, s, log, stats)
	}
	if err != nil {
		return w, err
	}
	return newBatcherFromConf(conf.Batching, w, mgr, log, stats)
}

//------------------------------------------------------------------------------
