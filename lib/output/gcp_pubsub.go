package output

import (
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/output/writer"
	"github.com/Jeffail/benthos/v3/lib/types"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeGCPPubSub] = TypeSpec{
		constructor: NewGCPPubSub,
		Description: `
Sends messages to a GCP Cloud Pub/Sub topic. Metadata from messages are sent as
attributes.`,
		Async: true,
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
