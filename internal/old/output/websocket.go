package output

import (
	"github.com/benthosdev/benthos/v4/internal/component/metrics"
	"github.com/benthosdev/benthos/v4/internal/component/output"
	"github.com/benthosdev/benthos/v4/internal/docs"
	"github.com/benthosdev/benthos/v4/internal/http/docs/auth"
	"github.com/benthosdev/benthos/v4/internal/interop"
	"github.com/benthosdev/benthos/v4/internal/log"
	"github.com/benthosdev/benthos/v4/internal/old/output/writer"
	btls "github.com/benthosdev/benthos/v4/internal/tls"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeWebsocket] = TypeSpec{
		constructor: fromSimpleConstructor(NewWebsocket),
		Summary: `
Sends messages to an HTTP server via a websocket connection.`,
		Config: docs.FieldComponent().WithChildren(
			docs.FieldString("url", "The URL to connect to."),
			btls.FieldSpec(),
		).WithChildren(auth.FieldSpecs()...),
		Categories: []string{
			"Network",
		},
	}
}

//------------------------------------------------------------------------------

// NewWebsocket creates a new Websocket output type.
func NewWebsocket(conf Config, mgr interop.Manager, log log.Modular, stats metrics.Type) (output.Streamed, error) {
	w, err := writer.NewWebsocket(conf.Websocket, log, stats)
	if err != nil {
		return nil, err
	}
	a, err := NewAsyncWriter(TypeWebsocket, 1, w, log, stats)
	if err != nil {
		return nil, err
	}
	return OnlySinglePayloads(a), nil
}

//------------------------------------------------------------------------------
