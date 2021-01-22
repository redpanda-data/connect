package output

import (
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
Sends messages as a continuous stream of line delimited data over a
(tcp/udp/unix) socket by connecting to a server.`,
		Description: `
Each message written is followed by a delimiter (defaults to '\n' if left empty)
and when sending multipart messages (message batches) the last message ends with
double delimiters. E.g. the messages "foo", "bar" and "baz" would be written as:

` + "```" + `
foo\n
bar\n
baz\n
` + "```" + `

Whereas a multipart message [ "foo", "bar", "baz" ] would be written as:

` + "```" + `
foo\n
bar\n
baz\n\n
` + "```" + ``,
		FieldSpecs: docs.FieldSpecs{
			docs.FieldCommon("network", "The network type to connect as.").HasOptions(
				"unix", "tcp", "udp",
			),
			docs.FieldCommon("address", "The address (or path) to connect to.", "/tmp/benthos.sock", "localhost:9000"),
		},
		Categories: []Category{
			CategoryNetwork,
		},
	}
}

// NewSocket creates a new Socket output type.
func NewSocket(conf Config, mgr types.Manager, log log.Modular, stats metrics.Type) (Type, error) {
	t, err := writer.NewSocket(conf.Socket, mgr, log, stats)
	if err != nil {
		return nil, err
	}
	return NewWriter(TypeSocket, t, log, stats)
}

//------------------------------------------------------------------------------
