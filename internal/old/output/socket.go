package output

import (
	"github.com/benthosdev/benthos/v4/internal/codec"
	"github.com/benthosdev/benthos/v4/internal/component/metrics"
	"github.com/benthosdev/benthos/v4/internal/component/output"
	"github.com/benthosdev/benthos/v4/internal/docs"
	"github.com/benthosdev/benthos/v4/internal/interop"
	"github.com/benthosdev/benthos/v4/internal/log"
	"github.com/benthosdev/benthos/v4/internal/old/output/writer"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeSocket] = TypeSpec{
		constructor: fromSimpleConstructor(NewSocket),
		Summary: `
Connects to a (tcp/udp/unix) server and sends a continuous stream of data, dividing messages according to the specified codec.`,
		Description: multipartCodecDoc,
		Config: docs.FieldComponent().WithChildren(
			docs.FieldString("network", "The network type to connect as.").HasOptions(
				"unix", "tcp", "udp",
			),
			docs.FieldString("address", "The address (or path) to connect to.", "/tmp/benthos.sock", "localhost:9000"),
			codec.WriterDocs,
		),
		Categories: []string{
			"Network",
		},
	}
}

// NewSocket creates a new Socket output type.
func NewSocket(conf Config, mgr interop.Manager, log log.Modular, stats metrics.Type) (output.Streamed, error) {
	t, err := writer.NewSocket(conf.Socket, mgr, log, stats)
	if err != nil {
		return nil, err
	}
	return NewAsyncWriter(TypeSocket, 1, t, log, stats)
}

//------------------------------------------------------------------------------
