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
	Constructors[TypeSQS] = TypeSpec{
		constructor: NewAmazonSQS,
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
		sanitiseConfigFunc: func(conf Config) (interface{}, error) {
			return sanitiseWithBatch(conf.SQS, conf.SQS.Batching)
		},
		Async:   true,
		Batches: true,
		FieldSpecs: docs.FieldSpecs{
			docs.FieldCommon("url", "The URL of the target SQS queue."),
			docs.FieldCommon("message_group_id", "An optional group ID to set for messages.").SupportsInterpolation(false),
			docs.FieldCommon("message_deduplication_id", "An optional deduplication ID to set for messages.").SupportsInterpolation(false),
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

// NewAmazonSQS creates a new AmazonSQS output type.
func NewAmazonSQS(conf Config, mgr types.Manager, log log.Modular, stats metrics.Type) (Type, error) {
	s, err := writer.NewAmazonSQS(conf.SQS, log, stats)
	if err != nil {
		return nil, err
	}
	var w Type
	if conf.SQS.MaxInFlight == 1 {
		w, err = NewWriter(
			TypeSQS, s, log, stats,
		)
	} else {
		w, err = NewAsyncWriter(
			TypeSQS, conf.SQS.MaxInFlight, s, log, stats,
		)
	}
	if err != nil {
		return w, err
	}
	return newBatcherFromConf(conf.SQS.Batching, w, mgr, log, stats)
}

//------------------------------------------------------------------------------
