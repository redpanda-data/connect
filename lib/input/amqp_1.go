package input

import (
	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/lib/input/reader"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/Jeffail/benthos/v3/lib/util/amqp/sasl"
	"github.com/Jeffail/benthos/v3/lib/util/tls"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeAMQP1] = TypeSpec{
		constructor: fromSimpleConstructor(NewAMQP1),
		Status:      docs.StatusBeta,
		Summary: `
Reads messages from an AMQP (1.0) server.`,
		Description: `
### Metadata

This input adds the following metadata fields to each message:

` + "``` text" + `
- amqp_content_type
- amqp_content_encoding
- amqp_creation_time
- All string typed message annotations
` + "```" + `

You can access these metadata fields using
[function interpolation](/docs/configuration/interpolation#metadata).`,
		Categories: []Category{
			CategoryServices,
		},
		FieldSpecs: docs.FieldSpecs{
			docs.FieldCommon("url",
				"A URL to connect to.",
				"amqp://localhost:5672/",
				"amqps://guest:guest@localhost:5672/",
			),
			docs.FieldCommon("source_address", "The source address to consume from.", "/foo", "queue:/bar", "topic:/baz"),
			docs.FieldAdvanced("azure_renew_lock", "Experimental: Azure service bus specific option to renew lock if processing takes more then configured lock time").AtVersion("3.45.0"),
			tls.FieldSpec(),
			sasl.FieldSpec(),
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
	return NewAsyncReader(TypeAMQP1, true, a, log, stats)
}

//------------------------------------------------------------------------------
