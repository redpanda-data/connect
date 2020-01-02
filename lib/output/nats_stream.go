package output

import (
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/output/writer"
	"github.com/Jeffail/benthos/v3/lib/types"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeNATSStream] = TypeSpec{
		constructor: NewNATSStream,
		Description: `
Publish to a NATS Stream subject.`,
		Async: true,
	}
}

//------------------------------------------------------------------------------

// NewNATSStream creates a new NATSStream output type.
func NewNATSStream(conf Config, mgr types.Manager, log log.Modular, stats metrics.Type) (Type, error) {
	w, err := writer.NewNATSStream(conf.NATSStream, log, stats)
	if err != nil {
		return nil, err
	}
	if conf.NATSStream.MaxInFlight == 1 {
		return NewWriter(TypeNATSStream, w, log, stats)
	}
	return NewAsyncWriter(TypeNATSStream, conf.NATSStream.MaxInFlight, w, log, stats)
}

//------------------------------------------------------------------------------
