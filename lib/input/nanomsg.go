package input

import (
	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/lib/input/reader"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeNanomsg] = TypeSpec{
		constructor: fromSimpleConstructor(NewNanomsg),
		Summary: `
Consumes messages via Nanomsg sockets (scalability protocols).`,
		Description: `
Currently only PULL and SUB sockets are supported.`,
		FieldSpecs: docs.FieldSpecs{
			docs.FieldCommon("urls", "A list of URLs to connect to (or as). If an item of the list contains commas it will be expanded into multiple URLs.").Array(),
			docs.FieldCommon("bind", "Whether the URLs provided should be connected to, or bound as."),
			docs.FieldCommon("socket_type", "The socket type to use.").HasOptions("PULL", "SUB"),
			docs.FieldString("sub_filters", "A list of subscription topic filters to use when consuming from a SUB socket. Specifying a single sub_filter of `''` will subscribe to everything.").Array(),
			docs.FieldAdvanced("poll_timeout", "The period to wait until a poll is abandoned and reattempted."),
		},
		Categories: []Category{
			CategoryNetwork,
		},
	}
}

//------------------------------------------------------------------------------

// NewNanomsg creates a new Nanomsg input type.
func NewNanomsg(conf Config, mgr types.Manager, log log.Modular, stats metrics.Type) (Type, error) {
	s, err := reader.NewScaleProto(conf.Nanomsg, log, stats)
	if err != nil {
		return nil, err
	}
	return NewAsyncReader(TypeNanomsg, true, reader.NewAsyncPreserver(s), log, stats)
}

//------------------------------------------------------------------------------
