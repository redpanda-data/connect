package output

import (
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/output/writer"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/Jeffail/benthos/v3/lib/util/amqp/sasl"
	"github.com/Jeffail/benthos/v3/lib/util/tls"
	"github.com/Jeffail/benthos/v3/lib/x/docs"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeAMQP1] = TypeSpec{
		constructor: NewAMQP1,
		Summary: `
BETA: This output is currently in a BETA stage and is therefore subject to
breaking configuration changes outside of major version releases.

Sends messages to an AMQP (1.0) server.`,
		Description: ``,
		Async:       true,
		FieldSpecs: docs.FieldSpecs{
			docs.FieldCommon("url",
				"A URL to connect to.",
				"amqp://localhost:5672/",
				"amqps://guest:guest@localhost:5672/",
			),
			docs.FieldCommon("target_address", "The target address to write to.", "/foo", "queue:/bar", "topic:/baz"),
			docs.FieldCommon("max_in_flight", "The maximum number of messages to have in flight at a given time. Increase this to improve throughput."),
			tls.FieldSpec(),
			sasl.FieldSpec(),
		},
	}
}

//------------------------------------------------------------------------------

// NewAMQP1 creates a new AMQP output type.
func NewAMQP1(conf Config, mgr types.Manager, log log.Modular, stats metrics.Type) (Type, error) {
	a, err := writer.NewAMQP1(conf.AMQP1, log, stats)
	if err != nil {
		return nil, err
	}
	return NewAsyncWriter(
		TypeAMQP1, conf.AMQP1.MaxInFlight, a, log, stats,
	)
}

//------------------------------------------------------------------------------
