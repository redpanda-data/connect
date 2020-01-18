package output

import (
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/output/writer"
	"github.com/Jeffail/benthos/v3/lib/types"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeNATS] = TypeSpec{
		constructor: NewNATS,
		Description: `
Publish to an NATS subject. NATS is at-most-once, so delivery is not guaranteed.
For at-least-once behaviour with NATS look at NATS Stream.

This output will interpolate functions within the subject field, you
can find a list of functions [here](/docs/configuration/interpolation#functions).`,
		Async: true,
	}
}

// NewNATS creates a new NATS output type.
func NewNATS(conf Config, mgr types.Manager, log log.Modular, stats metrics.Type) (Type, error) {
	w, err := writer.NewNATS(conf.NATS, log, stats)
	if err != nil {
		return nil, err
	}
	if conf.NATS.MaxInFlight == 1 {
		return NewWriter(TypeNATS, w, log, stats)
	}
	return NewAsyncWriter(TypeNATS, conf.NATS.MaxInFlight, w, log, stats)
}

//------------------------------------------------------------------------------
