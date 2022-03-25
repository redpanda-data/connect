package input

import (
	"github.com/benthosdev/benthos/v4/internal/component/input"
	"github.com/benthosdev/benthos/v4/internal/component/metrics"
	"github.com/benthosdev/benthos/v4/internal/docs"
	"github.com/benthosdev/benthos/v4/internal/interop"
	"github.com/benthosdev/benthos/v4/internal/log"
	"github.com/benthosdev/benthos/v4/internal/old/input/reader"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeNanomsg] = TypeSpec{
		constructor: fromSimpleConstructor(NewNanomsg),
		Summary: `
Consumes messages via Nanomsg sockets (scalability protocols).`,
		Description: `
Currently only PULL and SUB sockets are supported.`,
		Config: docs.FieldComponent().WithChildren(
			docs.FieldString("urls", "A list of URLs to connect to (or as). If an item of the list contains commas it will be expanded into multiple URLs.").Array(),
			docs.FieldBool("bind", "Whether the URLs provided should be connected to, or bound as."),
			docs.FieldString("socket_type", "The socket type to use.").HasOptions("PULL", "SUB"),
			docs.FieldString("sub_filters", "A list of subscription topic filters to use when consuming from a SUB socket. Specifying a single sub_filter of `''` will subscribe to everything.").Array(),
			docs.FieldString("poll_timeout", "The period to wait until a poll is abandoned and reattempted.").Advanced(),
		),
		Categories: []string{
			"Network",
		},
	}
}

//------------------------------------------------------------------------------

// NewNanomsg creates a new Nanomsg input type.
func NewNanomsg(conf Config, mgr interop.Manager, log log.Modular, stats metrics.Type) (input.Streamed, error) {
	s, err := reader.NewScaleProto(conf.Nanomsg, log, stats)
	if err != nil {
		return nil, err
	}
	return NewAsyncReader(TypeNanomsg, true, reader.NewAsyncPreserver(s), log, stats)
}

//------------------------------------------------------------------------------
