package output

import (
	"github.com/Jeffail/benthos/v3/internal/batch/policy"
	"github.com/Jeffail/benthos/v3/internal/component/metrics"
	"github.com/Jeffail/benthos/v3/internal/component/output"
	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/internal/impl/aws/session"
	"github.com/Jeffail/benthos/v3/internal/interop"
	"github.com/Jeffail/benthos/v3/internal/log"
	"github.com/Jeffail/benthos/v3/internal/metadata"
	"github.com/Jeffail/benthos/v3/internal/old/output/writer"
	"github.com/Jeffail/benthos/v3/internal/old/util/retries"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeAWSSQS] = TypeSpec{
		constructor: fromSimpleConstructor(func(conf Config, mgr interop.Manager, log log.Modular, stats metrics.Type) (output.Streamed, error) {
			return newAmazonSQS(TypeAWSSQS, conf.AWSSQS, mgr, log, stats)
		}),
		Version: "3.36.0",
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
[in this document](/docs/guides/cloud/aws).`,
		Async:   true,
		Batches: true,
		FieldSpecs: docs.FieldSpecs{
			docs.FieldCommon("url", "The URL of the target SQS queue."),
			docs.FieldCommon("message_group_id", "An optional group ID to set for messages.").IsInterpolated(),
			docs.FieldCommon("message_deduplication_id", "An optional deduplication ID to set for messages.").IsInterpolated(),
			docs.FieldCommon("max_in_flight", "The maximum number of messages to have in flight at a given time. Increase this to improve throughput."),
			docs.FieldCommon("metadata", "Specify criteria for which metadata values are sent as headers.").WithChildren(metadata.ExcludeFilterFields()...),
			policy.FieldSpec(),
		}.Merge(session.FieldSpecs()).Merge(retries.FieldSpecs()),
		Categories: []Category{
			CategoryServices,
			CategoryAWS,
		},
	}
}

//------------------------------------------------------------------------------

func newAmazonSQS(name string, conf writer.AmazonSQSConfig, mgr interop.Manager, log log.Modular, stats metrics.Type) (output.Streamed, error) {
	s, err := writer.NewAmazonSQSV2(conf, mgr, log, stats)
	if err != nil {
		return nil, err
	}
	w, err := NewAsyncWriter(name, conf.MaxInFlight, s, log, stats)
	if err != nil {
		return w, err
	}
	return NewBatcherFromConfig(conf.Batching, w, mgr, log, stats)
}

//------------------------------------------------------------------------------
