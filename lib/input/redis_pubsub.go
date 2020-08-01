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
	Constructors[TypeRedisPubSub] = TypeSpec{
		constructor: NewRedisPubSub,
		Summary: `
Consume from a Redis publish/subscribe channel using either the SUBSCRIBE or
PSUBSCRIBE commands.`,
		Description: `
In order to subscribe to channels using the ` + "`PSUBSCRIBE`" + ` command set
the field ` + "`use_patterns` to `true`" + `, then you can include glob-style
patterns in your channel names. For example:

- ` + "`h?llo`" + ` subscribes to hello, hallo and hxllo
- ` + "`h*llo`" + ` subscribes to hllo and heeeello
- ` + "`h[ae]llo`" + ` subscribes to hello and hallo, but not hillo

Use ` + "`\\`" + ` to escape special characters if you want to match them
verbatim.`,
		FieldSpecs: docs.FieldSpecs{
			docs.FieldCommon("url", "The URL of a Redis server to connect to.", "tcp://localhost:6379"),
			docs.FieldCommon("channels", "A list of channels to consume from."),
			docs.FieldCommon("use_patterns", "Whether to use the PSUBSCRIBE command."),
		},
	}
}

//------------------------------------------------------------------------------

// NewRedisPubSub creates a new RedisPubSub input type.
func NewRedisPubSub(conf Config, mgr types.Manager, log log.Modular, stats metrics.Type) (Type, error) {
	r, err := reader.NewRedisPubSub(conf.RedisPubSub, log, stats)
	if err != nil {
		return nil, err
	}
	return NewAsyncReader(TypeRedisPubSub, true, reader.NewAsyncPreserver(r), log, stats)
}

//------------------------------------------------------------------------------
