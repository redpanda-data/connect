package output

import (
	"github.com/benthosdev/benthos/v4/internal/component/metrics"
	"github.com/benthosdev/benthos/v4/internal/component/output"
	"github.com/benthosdev/benthos/v4/internal/docs"
	"github.com/benthosdev/benthos/v4/internal/impl/nats/auth"
	"github.com/benthosdev/benthos/v4/internal/interop"
	"github.com/benthosdev/benthos/v4/internal/log"
	"github.com/benthosdev/benthos/v4/internal/old/output/writer"
	"github.com/benthosdev/benthos/v4/internal/tls"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeNATS] = TypeSpec{
		constructor: fromSimpleConstructor(NewNATS),
		Summary: `
Publish to an NATS subject.`,
		Description: `
This output will interpolate functions within the subject field, you
can find a list of functions [here](/docs/configuration/interpolation#bloblang-queries).

` + auth.Description(),
		Async: true,
		Config: docs.FieldComponent().WithChildren(
			docs.FieldString(
				"urls",
				"A list of URLs to connect to. If an item of the list contains commas it will be expanded into multiple URLs.",
				[]string{"nats://127.0.0.1:4222"},
				[]string{"nats://username:password@127.0.0.1:4222"},
			).Array(),
			docs.FieldString("subject", "The subject to publish to.").IsInterpolated(),
			docs.FieldString("headers", "Explicit message headers to add to messages.",
				map[string]string{
					"Content-Type": "application/json",
					"Timestamp":    `${!meta("Timestamp")}`,
				},
			).IsInterpolated().Map(),
			docs.FieldInt("max_in_flight", "The maximum number of messages to have in flight at a given time. Increase this to improve throughput."),
			tls.FieldSpec(),
			auth.FieldSpec(),
		),
		Categories: []string{
			"Services",
		},
	}
}

// NewNATS creates a new NATS output type.
func NewNATS(conf Config, mgr interop.Manager, log log.Modular, stats metrics.Type) (output.Streamed, error) {
	w, err := writer.NewNATSV2(conf.NATS, mgr, log, stats)
	if err != nil {
		return nil, err
	}
	return NewAsyncWriter(TypeNATS, conf.NATS.MaxInFlight, w, log, stats)
}

//------------------------------------------------------------------------------
