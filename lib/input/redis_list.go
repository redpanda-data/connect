package input

import (
	"github.com/Jeffail/benthos/v3/lib/input/reader"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/Jeffail/benthos/v3/lib/x/docs"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeRedisList] = TypeSpec{
		constructor: NewRedisList,
		Summary: `
Pops messages from the beginning of a Redis list using the BLPop command.`,
		FieldSpecs: docs.FieldSpecs{
			docs.FieldCommon("url",
				"The URL of a Redis server to connect to. Database is optional and is supplied as the URL path.",
				"tcp://localhost:6379", "tcp://localhost:6379/1",
			),
			docs.FieldCommon("key", "The key of a list to read from."),
			docs.FieldAdvanced("timeout", "The length of time to poll for new messages before reattempting."),
		},
	}
}

//------------------------------------------------------------------------------

// NewRedisList creates a new Redis List input type.
func NewRedisList(conf Config, mgr types.Manager, log log.Modular, stats metrics.Type) (Type, error) {
	r, err := reader.NewRedisList(conf.RedisList, log, stats)
	if err != nil {
		return nil, err
	}
	return NewAsyncReader(TypeRedisList, true, reader.NewAsyncPreserver(r), log, stats)
}

//------------------------------------------------------------------------------
