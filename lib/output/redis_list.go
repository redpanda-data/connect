package output

import (
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/output/writer"
	"github.com/Jeffail/benthos/v3/lib/types"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeRedisList] = TypeSpec{
		constructor: NewRedisList,
		Description: `
Pushes messages onto the end of a Redis list (which is created if it doesn't
already exist) using the RPUSH command.`,
		Async: true,
	}
}

//------------------------------------------------------------------------------

// NewRedisList creates a new RedisList output type.
func NewRedisList(conf Config, mgr types.Manager, log log.Modular, stats metrics.Type) (Type, error) {
	w, err := writer.NewRedisList(conf.RedisList, log, stats)
	if err != nil {
		return nil, err
	}
	if conf.RedisList.MaxInFlight == 1 {
		return NewWriter(TypeRedisList, w, log, stats)
	}
	return NewAsyncWriter(TypeRedisList, conf.RedisList.MaxInFlight, w, log, stats)
}

//------------------------------------------------------------------------------
