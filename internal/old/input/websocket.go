package input

import (
	"github.com/benthosdev/benthos/v4/internal/component/input"
	"github.com/benthosdev/benthos/v4/internal/component/metrics"
	"github.com/benthosdev/benthos/v4/internal/docs"
	"github.com/benthosdev/benthos/v4/internal/http/docs/auth"
	"github.com/benthosdev/benthos/v4/internal/interop"
	"github.com/benthosdev/benthos/v4/internal/log"
	"github.com/benthosdev/benthos/v4/internal/old/input/reader"
	btls "github.com/benthosdev/benthos/v4/internal/tls"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeWebsocket] = TypeSpec{
		constructor: fromSimpleConstructor(NewWebsocket),
		Summary: `
Connects to a websocket server and continuously receives messages.`,
		Description: `
It is possible to configure an ` + "`open_message`" + `, which when set to a
non-empty string will be sent to the websocket server each time a connection is
first established.`,
		Config: docs.FieldComponent().WithChildren(
			docs.FieldString("url", "The URL to connect to.", "ws://localhost:4195/get/ws"),
			docs.FieldString("open_message", "An optional message to send to the server upon connection.").Advanced(),
			btls.FieldSpec(),
		).WithChildren(auth.FieldSpecs()...),
		Categories: []string{
			"Network",
		},
	}
}

//------------------------------------------------------------------------------

// NewWebsocket creates a new Websocket input type.
func NewWebsocket(conf Config, mgr interop.Manager, log log.Modular, stats metrics.Type) (input.Streamed, error) {
	ws, err := reader.NewWebsocket(conf.Websocket, log, stats)
	if err != nil {
		return nil, err
	}
	return NewAsyncReader("websocket", true, reader.NewAsyncPreserver(ws), log, stats)
}

//------------------------------------------------------------------------------
