package output

import (
	"github.com/Jeffail/benthos/v3/internal/component/output"
	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/internal/interop"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/output/writer"
	"github.com/Jeffail/benthos/v3/lib/util/tls"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeNSQ] = TypeSpec{
		constructor: fromSimpleConstructor(NewNSQ),
		Summary: `
Publish to an NSQ topic.`,
		Description: `
The ` + "`topic`" + ` field can be dynamically set using function interpolations
described [here](/docs/configuration/interpolation#bloblang-queries). When sending
batched messages these interpolations are performed per message part.`,
		Async: true,
		FieldSpecs: docs.FieldSpecs{
			docs.FieldCommon("nsqd_tcp_address", "The address of the target NSQD server."),
			docs.FieldCommon("topic", "The topic to publish to.").IsInterpolated(),
			docs.FieldCommon("user_agent", "A user agent string to connect with."),
			tls.FieldSpec(),
			docs.FieldCommon("max_in_flight", "The maximum number of messages to have in flight at a given time. Increase this to improve throughput."),
		},
		Categories: []Category{
			CategoryServices,
		},
	}
}

//------------------------------------------------------------------------------

// NewNSQ creates a new NSQ output type.
func NewNSQ(conf Config, mgr interop.Manager, log log.Modular, stats metrics.Type) (output.Streamed, error) {
	w, err := writer.NewNSQV2(conf.NSQ, mgr, log, stats)
	if err != nil {
		return nil, err
	}
	return NewAsyncWriter(TypeNSQ, conf.NSQ.MaxInFlight, w, log, stats)
}

//------------------------------------------------------------------------------
