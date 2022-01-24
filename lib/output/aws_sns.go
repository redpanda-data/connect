package output

import (
	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/internal/metadata"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/output/writer"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/Jeffail/benthos/v3/lib/util/aws/session"
)

//------------------------------------------------------------------------------

func init() {
	fields := docs.FieldSpecs{
		docs.FieldCommon("topic_arn", "The topic to publish to."),
		docs.FieldCommon("message_group_id", "An optional group ID to set for messages.").IsInterpolated().AtVersion("3.60.0"),
		docs.FieldCommon("message_deduplication_id", "An optional deduplication ID to set for messages.").IsInterpolated().AtVersion("3.60.0"),
		docs.FieldCommon("max_in_flight", "The maximum number of messages to have in flight at a given time. Increase this to improve throughput."),
		docs.FieldCommon("metadata", "Specify criteria for which metadata values are sent as headers.").WithChildren(metadata.ExcludeFilterFields()...).AtVersion("3.60.0"),
		docs.FieldAdvanced("timeout", "The maximum period to wait on an upload before abandoning it and reattempting."),
	}.Merge(session.FieldSpecs())

	Constructors[TypeAWSSNS] = TypeSpec{
		constructor: fromSimpleConstructor(func(conf Config, mgr types.Manager, log log.Modular, stats metrics.Type) (Type, error) {
			return newAmazonSNS(TypeAWSSNS, conf.AWSSNS, mgr, log, stats)
		}),
		Version: "3.36.0",
		Summary: `
Sends messages to an AWS SNS topic.`,
		Description: `
### Credentials

By default Benthos will use a shared credentials file when connecting to AWS
services. It's also possible to set them explicitly at the component level,
allowing you to transfer data across accounts. You can find out more
[in this document](/docs/guides/cloud/aws).`,
		Async:      true,
		FieldSpecs: fields,
		Categories: []Category{
			CategoryServices,
			CategoryAWS,
		},
	}
}

//------------------------------------------------------------------------------

func newAmazonSNS(name string, conf writer.SNSConfig, mgr types.Manager, log log.Modular, stats metrics.Type) (Type, error) {
	s, err := writer.NewSNSV2(conf, mgr, log, stats)
	if err != nil {
		return nil, err
	}
	a, err := NewAsyncWriter(name, conf.MaxInFlight, s, log, stats)
	if err != nil {
		return nil, err
	}
	return OnlySinglePayloads(a), nil
}

//------------------------------------------------------------------------------
