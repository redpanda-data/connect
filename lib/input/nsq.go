package input

import (
	"github.com/Jeffail/benthos/v3/internal/component/input"
	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/internal/interop"
	"github.com/Jeffail/benthos/v3/lib/input/reader"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/util/tls"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeNSQ] = TypeSpec{
		constructor: fromSimpleConstructor(NewNSQ),
		Summary: `
Subscribe to an NSQ instance topic and channel.`,
		FieldSpecs: docs.FieldSpecs{
			docs.FieldString("nsqd_tcp_addresses", "A list of nsqd addresses to connect to.").Array(),
			docs.FieldString("lookupd_http_addresses", "A list of nsqlookupd addresses to connect to.").Array(),
			tls.FieldSpec(),
			docs.FieldString("topic", "The topic to consume from."),
			docs.FieldString("channel", "The channel to consume from."),
			docs.FieldString("user_agent", "A user agent to assume when connecting."),
			docs.FieldInt("max_in_flight", "The maximum number of pending messages to consume at any given time."),
		},
		Categories: []Category{
			CategoryServices,
		},
	}
}

//------------------------------------------------------------------------------

// NewNSQ creates a new NSQ input type.
func NewNSQ(conf Config, mgr interop.Manager, log log.Modular, stats metrics.Type) (input.Streamed, error) {
	var n reader.Async
	var err error
	if n, err = reader.NewNSQ(conf.NSQ, log, stats); err != nil {
		return nil, err
	}
	return NewAsyncReader(TypeNSQ, true, n, log, stats)
}

//------------------------------------------------------------------------------
