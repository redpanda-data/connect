package output

import (
	"github.com/Jeffail/benthos/v3/internal/component/output"
	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/internal/http/docs/auth"
	"github.com/Jeffail/benthos/v3/internal/interop"
	btls "github.com/Jeffail/benthos/v3/internal/tls"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/output/writer"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeWebsocket] = TypeSpec{
		constructor: fromSimpleConstructor(NewWebsocket),
		Summary: `
Sends messages to an HTTP server via a websocket connection.`,
		FieldSpecs: docs.FieldSpecs{
			docs.FieldCommon("url", "The URL to connect to."),
			btls.FieldSpec(),
		}.Merge(auth.FieldSpecs()),
		Categories: []Category{
			CategoryNetwork,
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
