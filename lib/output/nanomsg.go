package output

import (
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/output/writer"
	"github.com/Jeffail/benthos/v3/lib/types"
)

//------------------------------------------------------------------------------

func init() {
	Constructors["nanomsg"] = TypeSpec{
		constructor: NewNanomsg,
		Description: `
The scalability protocols are common communication patterns. This output should
be compatible with any implementation, but specifically targets Nanomsg.

Currently only PUSH and PUB sockets are supported.`,
		Async: true,
	}
}

//------------------------------------------------------------------------------

// NewNanomsg creates a new Nanomsg output type.
func NewNanomsg(conf Config, mgr types.Manager, log log.Modular, stats metrics.Type) (Type, error) {
	s, err := writer.NewNanomsg(conf.Nanomsg, log, stats)
	if err != nil {
		return nil, err
	}
	if conf.Nanomsg.MaxInFlight == 1 {
		return NewWriter(TypeNanomsg, s, log, stats)
	}
	return NewAsyncWriter(TypeNanomsg, conf.Nanomsg.MaxInFlight, s, log, stats)
}

//------------------------------------------------------------------------------
