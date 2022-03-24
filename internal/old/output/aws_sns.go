package output

import (
	"github.com/benthosdev/benthos/v4/internal/component/metrics"
	"github.com/benthosdev/benthos/v4/internal/component/output"
	"github.com/benthosdev/benthos/v4/internal/docs"
	"github.com/benthosdev/benthos/v4/internal/impl/aws/session"
	"github.com/benthosdev/benthos/v4/internal/interop"
	"github.com/benthosdev/benthos/v4/internal/log"
	"github.com/benthosdev/benthos/v4/internal/metadata"
	"github.com/benthosdev/benthos/v4/internal/old/output/writer"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeAWSSNS] = TypeSpec{
		constructor: fromSimpleConstructor(func(conf Config, mgr interop.Manager, log log.Modular, stats metrics.Type) (output.Streamed, error) {
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
		Async: true,
		Config: docs.FieldComponent().WithChildren(
			docs.FieldString("topic_arn", "The topic to publish to."),
			docs.FieldString("message_group_id", "An optional group ID to set for messages.").IsInterpolated().AtVersion("3.60.0"),
			docs.FieldString("message_deduplication_id", "An optional deduplication ID to set for messages.").IsInterpolated().AtVersion("3.60.0"),
			docs.FieldInt("max_in_flight", "The maximum number of messages to have in flight at a given time. Increase this to improve throughput."),
			docs.FieldObject("metadata", "Specify criteria for which metadata values are sent as headers.").WithChildren(metadata.ExcludeFilterFields()...).AtVersion("3.60.0"),
			docs.FieldString("timeout", "The maximum period to wait on an upload before abandoning it and reattempting.").Advanced(),
		).WithChildren(session.FieldSpecs()...),
		Categories: []string{
			"Services",
			"AWS",
		},
	}
}

//------------------------------------------------------------------------------

func newAmazonSNS(name string, conf writer.SNSConfig, mgr interop.Manager, log log.Modular, stats metrics.Type) (output.Streamed, error) {
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
