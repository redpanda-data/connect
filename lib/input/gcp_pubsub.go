package input

import (
	"github.com/Jeffail/benthos/v3/lib/input/reader"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeGCPPubSub] = TypeSpec{
		constructor: NewGCPPubSub,
		Description: `
Consumes messages from a GCP Cloud Pub/Sub subscription.

Messages consumed by this input can be processed in parallel, meaning a single
instance of this input can utilise any number of threads within a
` + "`pipeline`" + ` section of a config.

Use the ` + "`batching`" + ` fields to configure an optional
[batching policy](../batching.md#batch-policy).

### Metadata

This input adds the following metadata fields to each message:

` + "``` text" + `
- gcp_pubsub_publish_time_unix
- All message attributes
` + "```" + `

You can access these metadata fields using
[function interpolation](../config_interpolation.md#metadata).`,
		sanitiseConfigFunc: func(conf Config) (interface{}, error) {
			return sanitiseWithBatch(conf.GCPPubSub, conf.GCPPubSub.Batching)
		},
	}
}

//------------------------------------------------------------------------------

// NewGCPPubSub creates a new GCP Cloud Pub/Sub input type.
func NewGCPPubSub(conf Config, mgr types.Manager, log log.Modular, stats metrics.Type) (Type, error) {
	// TODO: V4 Remove this.
	if conf.GCPPubSub.MaxBatchCount > 1 {
		log.Warnf("Field '%v.max_batch_count' is deprecated, use '%v.batching.count' instead.\n", conf.Type, conf.Type)
		conf.GCPPubSub.Batching.Count = conf.GCPPubSub.MaxBatchCount
	}
	var c reader.Async
	var err error
	if c, err = reader.NewGCPPubSub(conf.GCPPubSub, log, stats); err != nil {
		return nil, err
	}
	if c, err = reader.NewAsyncBatcher(conf.GCPPubSub.Batching, c, mgr, log, stats); err != nil {
		return nil, err
	}
	c = reader.NewAsyncBundleUnacks(c)
	return NewAsyncReader(TypeGCPPubSub, true, c, log, stats)
}

//------------------------------------------------------------------------------
