package output

import (
	"github.com/Jeffail/benthos/v3/internal/component/output"
	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/internal/metadata"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/output/writer"
	"github.com/Jeffail/benthos/v3/lib/types"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeGCPPubSub] = TypeSpec{
		constructor: fromSimpleConstructor(NewGCPPubSub),
		Summary: `
Sends messages to a GCP Cloud Pub/Sub topic. [Metadata](/docs/configuration/metadata) from messages are sent as attributes.`,
		Description: `
For information on how to set up credentials check out [this guide](https://cloud.google.com/docs/authentication/production).

### Troubleshooting

If you're consistently seeing ` + "`Failed to send message to gcp_pubsub: context deadline exceeded`" + ` error logs without any further information it is possible that you are encountering https://github.com/Jeffail/benthos/issues/1042, which occurs when metadata values contain characters that are not valid utf-8. This can frequently occur when consuming from Kafka as the key metadata field may be populated with an arbitrary binary value, but this issue is not exclusive to Kafka.

If you are blocked by this issue then a work around is to delete either the specific problematic keys:

` + "```yaml" + `
pipeline:
  processors:
    - bloblang: |
        meta kafka_key = deleted()
` + "```" + `

Or delete all keys with:

` + "```yaml" + `
pipeline:
  processors:
    - bloblang: meta = deleted()
` + "```" + ``,
		Async: true,
		FieldSpecs: docs.FieldSpecs{
			docs.FieldCommon("project", "The project ID of the topic to publish to."),
			docs.FieldCommon("topic", "The topic to publish to.").IsInterpolated(),
			docs.FieldCommon("max_in_flight", "The maximum number of messages to have in flight at a given time. Increase this to improve throughput."),
			docs.FieldAdvanced("publish_timeout", "The maximum length of time to wait before abandoning a publish attempt for a message.", "10s", "5m", "60m"),
			docs.FieldAdvanced("ordering_key", "The ordering key to use for publishing messages.").IsInterpolated(),
			docs.FieldCommon("metadata", "Specify criteria for which metadata values are sent as attributes.").WithChildren(metadata.ExcludeFilterFields()...),
		},
		Categories: []Category{
			CategoryServices,
			CategoryGCP,
		},
	}
}

//------------------------------------------------------------------------------

// NewGCPPubSub creates a new GCPPubSub output type.
func NewGCPPubSub(conf Config, mgr types.Manager, log log.Modular, stats metrics.Type) (output.Streamed, error) {
	a, err := writer.NewGCPPubSubV2(conf.GCPPubSub, mgr, log, stats)
	if err != nil {
		return nil, err
	}
	w, err := NewAsyncWriter(
		TypeGCPPubSub, conf.GCPPubSub.MaxInFlight, a, log, stats,
	)
	if err != nil {
		return nil, err
	}
	return OnlySinglePayloads(w), nil
}

//------------------------------------------------------------------------------
