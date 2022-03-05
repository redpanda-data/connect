package input

import (
	"github.com/Jeffail/benthos/v3/internal/component/input"
	"github.com/Jeffail/benthos/v3/internal/component/metrics"
	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/internal/impl/redis/old"
	"github.com/Jeffail/benthos/v3/internal/interop"
	"github.com/Jeffail/benthos/v3/internal/log"
	"github.com/Jeffail/benthos/v3/internal/old/input/reader"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeRedisPubSub] = TypeSpec{
		constructor: fromSimpleConstructor(NewRedisPubSub),
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
		FieldSpecs: old.ConfigDocs().Add(
			docs.FieldString("channels", "A list of channels to consume from.").Array(),
			docs.FieldBool("use_patterns", "Whether to use the PSUBSCRIBE command."),
		),
		Categories: []Category{
			CategoryServices,
		},
	}
}

//------------------------------------------------------------------------------

// NewRedisPubSub creates a new RedisPubSub input type.
func NewRedisPubSub(conf Config, mgr interop.Manager, log log.Modular, stats metrics.Type) (input.Streamed, error) {
	r, err := reader.NewRedisPubSub(conf.RedisPubSub, log, stats)
	if err != nil {
		return nil, err
	}
	return NewAsyncReader(TypeRedisPubSub, true, reader.NewAsyncPreserver(r), log, stats)
}

//------------------------------------------------------------------------------
