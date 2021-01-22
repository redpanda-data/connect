package output

import (
	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/output/writer"
	"github.com/Jeffail/benthos/v3/lib/types"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeUDP] = TypeSpec{
		constructor: fromSimpleConstructor(NewUDP),
		Description: `
Sends messages as a continuous stream of line delimited data over UDP by
connecting to a server.

If batched messages are sent the final message of the batch will be followed by
two line breaks in order to indicate the end of the batch.`,
		Status: docs.StatusDeprecated,
	}
}

// NewUDP creates a new UDP output type.
func NewUDP(conf Config, mgr types.Manager, log log.Modular, stats metrics.Type) (Type, error) {
	log.Warnln("The udp output is deprecated, please use socket instead.")
	t, err := writer.NewUDP(conf.UDP, mgr, log, stats)
	if err != nil {
		return nil, err
	}
	return NewWriter(TypeUDP, t, log, stats)
}

//------------------------------------------------------------------------------
