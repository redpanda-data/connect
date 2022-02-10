package input

import (
	"github.com/Jeffail/benthos/v3/internal/component/input"
	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/internal/impl/redis/old"
	"github.com/Jeffail/benthos/v3/lib/input/reader"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeRedisList] = TypeSpec{
		constructor: fromSimpleConstructor(NewRedisList),
		Summary: `
Pops messages from the beginning of a Redis list using the BLPop command.`,
		FieldSpecs: old.ConfigDocs().Add(
			docs.FieldCommon("key", "The key of a list to read from."),
			docs.FieldAdvanced("timeout", "The length of time to poll for new messages before reattempting."),
		),
		Categories: []Category{
			CategoryServices,
		},
	}
}

//------------------------------------------------------------------------------

// NewRedisList creates a new Redis List input type.
func NewRedisList(conf Config, mgr types.Manager, log log.Modular, stats metrics.Type) (input.Streamed, error) {
	r, err := reader.NewRedisList(conf.RedisList, log, stats)
	if err != nil {
		return nil, err
	}
	return NewAsyncReader(TypeRedisList, true, reader.NewAsyncPreserver(r), log, stats)
}

//------------------------------------------------------------------------------
