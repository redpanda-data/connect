package output

import (
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/output/writer"
	"github.com/Jeffail/benthos/v3/lib/types"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeRedisPubSub] = TypeSpec{
		constructor: NewRedisPubSub,
		Description: `
Publishes messages through the Redis PubSub model. It is not possible to
guarantee that messages have been received.

This output will interpolate functions within the channel field, you
can find a list of functions [here](../config_interpolation.md#functions).`,
		Async: true,
	}
}

//------------------------------------------------------------------------------

// NewRedisPubSub creates a new RedisPubSub output type.
func NewRedisPubSub(conf Config, mgr types.Manager, log log.Modular, stats metrics.Type) (Type, error) {
	w, err := writer.NewRedisPubSub(conf.RedisPubSub, log, stats)
	if err != nil {
		return nil, err
	}
	if conf.RedisPubSub.MaxInFlight == 1 {
		return NewWriter(TypeRedisPubSub, w, log, stats)
	}
	return NewAsyncWriter(TypeRedisPubSub, conf.RedisPubSub.MaxInFlight, w, log, stats)
}

//------------------------------------------------------------------------------
