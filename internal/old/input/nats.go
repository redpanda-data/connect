package input

import (
	"github.com/benthosdev/benthos/v4/internal/component/input"
	"github.com/benthosdev/benthos/v4/internal/component/metrics"
	"github.com/benthosdev/benthos/v4/internal/docs"
	"github.com/benthosdev/benthos/v4/internal/impl/nats/auth"
	"github.com/benthosdev/benthos/v4/internal/interop"
	"github.com/benthosdev/benthos/v4/internal/log"
	"github.com/benthosdev/benthos/v4/internal/old/input/reader"
	"github.com/benthosdev/benthos/v4/internal/tls"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeNATS] = TypeSpec{
		constructor: fromSimpleConstructor(NewNATS),
		Summary: `
Subscribe to a NATS subject.`,
		Description: `
### Metadata

This input adds the following metadata fields to each message:

` + "``` text" + `
- nats_subject
- All message headers (when supported by the connection)
` + "```" + `

You can access these metadata fields using
[function interpolation](/docs/configuration/interpolation#metadata).

` + auth.Description(),
		FieldSpecs: docs.FieldSpecs{
			docs.FieldString(
				"urls",
				"A list of URLs to connect to. If an item of the list contains commas it will be expanded into multiple URLs.",
				[]string{"nats://127.0.0.1:4222"},
				[]string{"nats://username:password@127.0.0.1:4222"},
			).Array(),
			docs.FieldString("queue", "The queue to consume from."),
			docs.FieldString("subject", "A subject to consume from."),
			docs.FieldInt("prefetch_count", "The maximum number of messages to pull at a time.").Advanced(),
			tls.FieldSpec(),
			auth.FieldSpec(),
		},
		Categories: []Category{
			CategoryServices,
		},
	}
}

//------------------------------------------------------------------------------

// NewNATS creates a new NATS input type.
func NewNATS(conf Config, mgr interop.Manager, log log.Modular, stats metrics.Type) (input.Streamed, error) {
	n, err := reader.NewNATS(conf.NATS, log, stats)
	if err != nil {
		return nil, err
	}
	return NewAsyncReader(TypeNATS, true, reader.NewAsyncPreserver(n), log, stats)
}

//------------------------------------------------------------------------------
