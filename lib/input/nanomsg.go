package input

import (
	"github.com/Jeffail/benthos/v3/lib/input/reader"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeNanomsg] = TypeSpec{
		constructor: NewNanomsg,
		Description: `
The scalability protocols are common communication patterns. This input should
be compatible with any implementation, but specifically targets Nanomsg.

Messages consumed by this input can be processed in parallel, meaning a single
instance of this input can utilise any number of threads within a
` + "`pipeline`" + ` section of a config.

Currently only PULL and SUB sockets are supported.`,
	}
}

//------------------------------------------------------------------------------

// NewNanomsg creates a new Nanomsg input type.
func NewNanomsg(conf Config, mgr types.Manager, log log.Modular, stats metrics.Type) (Type, error) {
	s, err := reader.NewScaleProto(conf.Nanomsg, log, stats)
	if err != nil {
		return nil, err
	}
	return NewAsyncReader(TypeNanomsg, true, reader.NewAsyncPreserver(s), log, stats)
}

//------------------------------------------------------------------------------
