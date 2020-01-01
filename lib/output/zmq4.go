// +build ZMQ4

package output

import (
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/output/writer"
	"github.com/Jeffail/benthos/v3/lib/types"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeZMQ4] = TypeSpec{
		constructor: NewZMQ4,
		Description: `
The zmq4 output type attempts to send messages to a ZMQ4 port, currently only
PUSH and PUB sockets are supported.`,
	}
}

//------------------------------------------------------------------------------

// NewZMQ4 creates a new ZMQ4 output type.
func NewZMQ4(conf Config, mgr types.Manager, log log.Modular, stats metrics.Type) (Type, error) {
	z, err := writer.NewZMQ4(conf.ZMQ4, log, stats)
	if err != nil {
		return nil, err
	}
	return NewWriter(
		"zmq4", z, log, stats,
	)
}

//------------------------------------------------------------------------------
