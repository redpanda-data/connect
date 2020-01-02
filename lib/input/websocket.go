package input

import (
	"github.com/Jeffail/benthos/v3/lib/input/reader"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeWebsocket] = TypeSpec{
		constructor: NewWebsocket,
		Description: `
Connects to a websocket server and continuously receives messages.

Messages consumed by this input can be processed in parallel, meaning a single
instance of this input can utilise any number of threads within a
` + "`pipeline`" + ` section of a config.

It is possible to configure an ` + "`open_message`" + `, which when set to a
non-empty string will be sent to the websocket server each time a connection is
first established.`,
	}
}

//------------------------------------------------------------------------------

// NewWebsocket creates a new Websocket input type.
func NewWebsocket(conf Config, mgr types.Manager, log log.Modular, stats metrics.Type) (Type, error) {
	ws, err := reader.NewWebsocket(conf.Websocket, log, stats)
	if err != nil {
		return nil, err
	}
	return NewAsyncReader("websocket", true, reader.NewAsyncPreserver(ws), log, stats)
}

//------------------------------------------------------------------------------
