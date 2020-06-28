package input

import (
	"github.com/Jeffail/benthos/v3/lib/input/reader"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/Jeffail/benthos/v3/lib/util/tls"
	"github.com/Jeffail/benthos/v3/lib/x/docs"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeAMQP1] = TypeSpec{
		constructor: NewAMQP1,
		Summary: `
BETA: This input is currently in a BETA stage and is therefore subject to
breaking configuration changes outside of major version releases.

Reads messages from an AMQP (1.0) server.`,
		Description: `
### Metadata

This input adds the following metadata fields to each message:

` + "``` text" + `
- amqp_content_type
- amqp_content_encoding
- amqp_creation_time
` + "```" + `

You can access these metadata fields using
[function interpolation](/docs/configuration/interpolation#metadata).`,
		FieldSpecs: docs.FieldSpecs{
			docs.FieldCommon("url",
				"A URL to connect to.",
				"amqp://localhost:5672/",
				"amqps://guest:guest@localhost:5672/",
			),
			docs.FieldCommon("source_address", "The source address to consume from.", "/foo", "queue:/bar", "topic:/baz"),
			tls.FieldSpec(),
		},
	}
}

//------------------------------------------------------------------------------

// NewAMQP1 creates a new AMQP1 input type.
func NewAMQP1(conf Config, mgr types.Manager, log log.Modular, stats metrics.Type) (Type, error) {
	var a reader.Async
	var err error
	if a, err = reader.NewAMQP1(conf.AMQP1, log, stats); err != nil {
		return nil, err
	}
	a = reader.NewAsyncBundleUnacks(a)
	return NewAsyncReader(TypeAMQP1, true, a, log, stats)
}

//------------------------------------------------------------------------------
