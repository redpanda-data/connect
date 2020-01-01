package input

import (
	"github.com/Jeffail/benthos/v3/lib/input/reader"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeNSQ] = TypeSpec{
		constructor: NewNSQ,
		Description: `
Subscribe to an NSQ instance topic and channel.

Messages consumed by this input can be processed in parallel, meaning a single
instance of this input can utilise any number of threads within a
` + "`pipeline`" + ` section of a config.

Use the ` + "`batching`" + ` fields to configure an optional
[batching policy](../batching.md#batch-policy).`,
		sanitiseConfigFunc: func(conf Config) (interface{}, error) {
			return sanitiseWithBatch(conf.NSQ, conf.NSQ.Batching)
		},
	}
}

//------------------------------------------------------------------------------

// NewNSQ creates a new NSQ input type.
func NewNSQ(conf Config, mgr types.Manager, log log.Modular, stats metrics.Type) (Type, error) {
	var n reader.Async
	var err error
	if n, err = reader.NewNSQ(conf.NSQ, log, stats); err != nil {
		return nil, err
	}
	if n, err = reader.NewAsyncBatcher(conf.NSQ.Batching, n, mgr, log, stats); err != nil {
		return nil, err
	}
	n = reader.NewAsyncBundleUnacks(n)
	return NewAsyncReader(TypeNSQ, true, n, log, stats)
}

//------------------------------------------------------------------------------
