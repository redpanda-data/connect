package output

import (
	"github.com/Jeffail/benthos/v3/internal/codec"
	"github.com/Jeffail/benthos/v3/internal/component/output"
	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/output/writer"
	"github.com/Jeffail/benthos/v3/lib/types"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeSocket] = TypeSpec{
		constructor: fromSimpleConstructor(NewSocket),
		Summary: `
Connects to a (tcp/udp/unix) server and sends a continuous stream of data, dividing messages according to the specified codec.`,
		Description: multipartCodecDoc,
		FieldSpecs: docs.FieldSpecs{
			docs.FieldCommon("network", "The network type to connect as.").HasOptions(
				"unix", "tcp", "udp",
			),
			docs.FieldCommon("address", "The address (or path) to connect to.", "/tmp/benthos.sock", "localhost:9000"),
			codec.WriterDocs,
		},
		Categories: []Category{
			CategoryNetwork,
		},
	}
}

// NewSocket creates a new Socket output type.
func NewSocket(conf Config, mgr types.Manager, log log.Modular, stats metrics.Type) (output.Streamed, error) {
	t, err := writer.NewSocket(conf.Socket, mgr, log, stats)
	if err != nil {
		return nil, err
	}
	return NewAsyncWriter(TypeSocket, 1, t, log, stats)
}

//------------------------------------------------------------------------------
