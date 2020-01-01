package input

import (
	"github.com/Jeffail/benthos/v3/lib/input/reader"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeRedisPubSub] = TypeSpec{
		constructor: NewRedisPubSub,
		Description: `
Redis supports a publish/subscribe model, it's possible to subscribe to multiple
channels using this input.

In order to subscribe to channels using the ` + "`PSUBSCRIBE`" + ` command set
the field ` + "`use_patterns` to `true`" + `, then you can include glob-style
patterns in your channel names. For example:

- ` + "`h?llo`" + ` subscribes to hello, hallo and hxllo
- ` + "`h*llo`" + ` subscribes to hllo and heeeello
- ` + "`h[ae]llo`" + ` subscribes to hello and hallo, but not hillo

Use ` + "`\\`" + ` to escape special characters if you want to match them
verbatim.`,
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
