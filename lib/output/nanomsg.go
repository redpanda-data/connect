package output

import (
	"github.com/Jeffail/benthos/v3/internal/component/output"
	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/output/writer"
	"github.com/Jeffail/benthos/v3/lib/types"
)

//------------------------------------------------------------------------------

func init() {
	Constructors["nanomsg"] = TypeSpec{
		constructor: fromSimpleConstructor(NewNanomsg),
		Summary: `
Send messages over a Nanomsg socket.`,
		Description: `
Currently only PUSH and PUB sockets are supported.`,
		Async: true,
		FieldSpecs: docs.FieldSpecs{
			docs.FieldString("urls", "A list of URLs to connect to. If an item of the list contains commas it will be expanded into multiple URLs.", []string{"tcp://localhost:5556"}).Array(),
			docs.FieldCommon("bind", "Whether the URLs listed should be bind (otherwise they are connected to)."),
			docs.FieldCommon("socket_type", "The socket type to send with.").HasOptions("PUSH", "PUB"),
			docs.FieldCommon("poll_timeout", "The maximum period of time to wait for a message to send before the request is abandoned and reattempted."),
			docs.FieldCommon("max_in_flight", "The maximum number of messages to have in flight at a given time. Increase this to improve throughput."),
		},
		Categories: []Category{
			CategoryNetwork,
		},
	}
}

//------------------------------------------------------------------------------

// NewNanomsg creates a new Nanomsg output type.
func NewNanomsg(conf Config, mgr types.Manager, log log.Modular, stats metrics.Type) (output.Streamed, error) {
	s, err := writer.NewNanomsg(conf.Nanomsg, log, stats)
	if err != nil {
		return nil, err
	}
	a, err := NewAsyncWriter(TypeNanomsg, conf.Nanomsg.MaxInFlight, s, log, stats)
	if err != nil {
		return nil, err
	}
	return OnlySinglePayloads(a), nil
}

//------------------------------------------------------------------------------
