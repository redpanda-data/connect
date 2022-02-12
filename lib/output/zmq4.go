//go:build ZMQ4
// +build ZMQ4

package output

import (
	"github.com/Jeffail/benthos/v3/internal/component/output"
	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/internal/interop"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/output/writer"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeZMQ4] = TypeSpec{
		constructor: fromSimpleConstructor(NewZMQ4),
		Summary: `
The zmq4 output type attempts to send messages to a ZMQ4 port, currently only
PUSH and PUB sockets are supported.`,
		Description: `
ZMQ4 is supported but currently depends on C bindings. Since this is an
annoyance when building or using Benthos it is not compiled by default.

There is a specific docker tag postfix ` + "`-cgo`" + ` for C builds containing
ZMQ support.

You can also build it into your project by getting libzmq installed on your
machine, then build with the tag:

` + "```sh" + `
go install -tags "ZMQ4" github.com/Jeffail/benthos/v3/cmd/benthos
` + "```" + ``,
		FieldSpecs: docs.FieldSpecs{
			docs.FieldString("urls", "A list of URLs to connect to. If an item of the list contains commas it will be expanded into multiple URLs.", []string{"tcp://localhost:5556"}).Array(),
			docs.FieldBool("bind", "Whether the URLs listed should be bind (otherwise they are connected to)."),
			docs.FieldString("socket_type", "The socket type to send with.").HasOptions("PUSH", "PUB"),
			docs.FieldInt("high_water_mark", "The message high water mark to use.").Advanced(),
			docs.FieldString("poll_timeout", "The maximum period of time to wait for a message to send before the request is abandoned and reattempted.").Advanced(),
		},
		Categories: []Category{
			CategoryNetwork,
		},
	}
}

//------------------------------------------------------------------------------

// NewZMQ4 creates a new ZMQ4 output type.
func NewZMQ4(conf Config, mgr interop.Manager, log log.Modular, stats metrics.Type) (output.Streamed, error) {
	z, err := writer.NewZMQ4(conf.ZMQ4, log, stats)
	if err != nil {
		return nil, err
	}
	s, err := NewWriter(
		"zmq4", z, log, stats,
	)
	if err != nil {
		return nil, err
	}
	return OnlySinglePayloads(s), nil
}

//------------------------------------------------------------------------------
