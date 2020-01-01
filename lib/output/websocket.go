package output

import (
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/output/writer"
	"github.com/Jeffail/benthos/v3/lib/types"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeWebsocket] = TypeSpec{
		constructor: NewWebsocket,
		Description: `
Sends messages to an HTTP server via a websocket connection.`,
	}
}

//------------------------------------------------------------------------------

// NewWebsocket creates a new Websocket output type.
func NewWebsocket(conf Config, mgr types.Manager, log log.Modular, stats metrics.Type) (Type, error) {
	w, err := writer.NewWebsocket(conf.Websocket, log, stats)
	if err != nil {
		return nil, err
	}
	return NewWriter(TypeWebsocket, w, log, stats)
}

//------------------------------------------------------------------------------
