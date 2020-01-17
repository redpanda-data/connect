package output

import (
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/output/writer"
	"github.com/Jeffail/benthos/v3/lib/types"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeSocket] = TypeSpec{
		constructor: NewSocket,
		Description: `
Sends messages as a continuous stream of line delimited data over a
(tcp/udp/unix) socket by connecting to a server.

If batched messages are sent the final message of the batch will be followed by
two line breaks in order to indicate the end of the batch.`,
	}
}

// NewSocket creates a new Socket output type.
func NewSocket(conf Config, mgr types.Manager, log log.Modular, stats metrics.Type) (Type, error) {
	t, err := writer.NewSocket(conf.Socket, mgr, log, stats)
	if err != nil {
		return nil, err
	}
	return NewWriter(TypeSocket, t, log, stats)
}

//------------------------------------------------------------------------------
