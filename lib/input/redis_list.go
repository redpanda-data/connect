package input

import (
	"github.com/Jeffail/benthos/v3/lib/input/reader"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeRedisList] = TypeSpec{
		constructor: NewRedisList,
		Description: `
Pops messages from the beginning of a Redis list using the BLPop command.`,
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
