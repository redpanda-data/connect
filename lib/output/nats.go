package output

import (
	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/internal/impl/nats/auth"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/output/writer"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/Jeffail/benthos/v3/lib/util/tls"
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
		FieldSpecs: docs.FieldSpecs{
			docs.FieldCommon(
				"urls",
				"A list of URLs to connect to. If an item of the list contains commas it will be expanded into multiple URLs.",
				[]string{"nats://127.0.0.1:4222"},
				[]string{"nats://username:password@127.0.0.1:4222"},
			).Array(),
			docs.FieldCommon("subject", "The subject to publish to.").IsInterpolated(),
			docs.FieldString("headers", "message headers to include",
				map[string]string{
					"Content-Type": "application/json",
					"Timestamp":    `${!meta("Timestamp")}`,
				},
			).IsInterpolated().Map(),
			docs.FieldCommon("max_in_flight", "The maximum number of messages to have in flight at a given time. Increase this to improve throughput."),
			tls.FieldSpec(),
			auth.FieldSpec(),
		},
		Categories: []Category{
			CategoryServices,
		},
	}
}

// NewNATS creates a new NATS output type.
func NewNATS(conf Config, mgr types.Manager, log log.Modular, stats metrics.Type) (Type, error) {
	w, err := writer.NewNATSV2(conf.NATS, mgr, log, stats)
	if err != nil {
		return nil, err
	}
	if conf.NATS.MaxInFlight == 1 {
		return NewWriter(TypeNATS, w, log, stats)
	}
	return NewAsyncWriter(TypeNATS, conf.NATS.MaxInFlight, w, log, stats)
}

//------------------------------------------------------------------------------
