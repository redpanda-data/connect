package output

import (
	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/output/writer"
	"github.com/Jeffail/benthos/v3/lib/types"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeGCPPubSub] = TypeSpec{
		constructor: NewGCPPubSub,
		Summary: `
Sends messages to a GCP Cloud Pub/Sub topic. [Metadata](/docs/configuration/metadata)
from messages are sent as attributes.`,
		Description: `
For information on how to set up credentials check out
[this guide](https://cloud.google.com/docs/authentication/production).`,
		Async: true,
		FieldSpecs: docs.FieldSpecs{
			docs.FieldCommon("project", "The project ID of the topic to publish to."),
			docs.FieldCommon("topic", "The topic to publish to.").SupportsInterpolation(false),
			docs.FieldCommon("max_in_flight", "The maximum number of messages to have in flight at a given time. Increase this to improve throughput."),
		},
		Categories: []Category{
			CategoryServices,
			CategoryGCP,
		},
	}
}

//------------------------------------------------------------------------------

// NewGCPPubSub creates a new GCPPubSub output type.
func NewGCPPubSub(conf Config, mgr types.Manager, log log.Modular, stats metrics.Type) (Type, error) {
	a, err := writer.NewGCPPubSub(conf.GCPPubSub, log, stats)
	if err != nil {
		return nil, err
	}
	if conf.GCPPubSub.MaxInFlight == 1 {
		return NewWriter(
			TypeGCPPubSub, a, log, stats,
		)
	}
	return NewAsyncWriter(
		TypeGCPPubSub, conf.GCPPubSub.MaxInFlight, a, log, stats,
	)
}

//------------------------------------------------------------------------------
