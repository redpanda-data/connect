package output

import (
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/output/writer"
	"github.com/Jeffail/benthos/v3/lib/types"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeTCP] = TypeSpec{
		constructor: NewTCP,
		Description: `
Sends messages as a continuous stream of line delimited data over TCP by
connecting to a server.

If batched messages are sent the final message of the batch will be followed by
two line breaks in order to indicate the end of the batch.`,
	}
}

// NewTCP creates a new TCP output type.
func NewTCP(conf Config, mgr types.Manager, log log.Modular, stats metrics.Type) (Type, error) {
	t, err := writer.NewTCP(conf.TCP, mgr, log, stats)
	if err != nil {
		return nil, err
	}
	return NewWriter(TypeTCP, t, log, stats)
}

//------------------------------------------------------------------------------
