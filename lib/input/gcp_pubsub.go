package input

import (
	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/lib/input/reader"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message/batch"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeGCPPubSub] = TypeSpec{
		constructor: fromSimpleConstructor(NewGCPPubSub),
		Summary: `
Consumes messages from a GCP Cloud Pub/Sub subscription.`,
		Description: `
For information on how to set up credentials check out
[this guide](https://cloud.google.com/docs/authentication/production).

### Metadata

This input adds the following metadata fields to each message:

` + "``` text" + `
- gcp_pubsub_publish_time_unix
- All message attributes
` + "```" + `

You can access these metadata fields using
[function interpolation](/docs/configuration/interpolation#metadata).`,
		Categories: []Category{
			CategoryServices,
			CategoryGCP,
		},
		FieldSpecs: docs.FieldSpecs{
			docs.FieldCommon("project", "The project ID of the target subscription."),
			docs.FieldCommon("subscription", "The target subscription ID."),
			docs.FieldCommon("sync", "Enable synchronous pull mode."),
			docs.FieldCommon("max_outstanding_messages", "The maximum number of outstanding pending messages to be consumed at a given time."),
			docs.FieldCommon("max_outstanding_bytes", "The maximum number of outstanding pending messages to be consumed measured in bytes."),
			func() docs.FieldSpec {
				b := batch.FieldSpec()
				b.IsDeprecated = true
				return b
			}(),
			docs.FieldDeprecated("max_batch_count"),
		},
	}
}

//------------------------------------------------------------------------------

// NewGCPPubSub creates a new GCP Cloud Pub/Sub input type.
func NewGCPPubSub(conf Config, mgr types.Manager, log log.Modular, stats metrics.Type) (Type, error) {
	var c reader.Async
	var err error
	if c, err = reader.NewGCPPubSub(conf.GCPPubSub, log, stats); err != nil {
		return nil, err
	}
	return NewAsyncReader(TypeGCPPubSub, true, c, log, stats)
}

//------------------------------------------------------------------------------
