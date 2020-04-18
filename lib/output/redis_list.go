package output

import (
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/output/writer"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/Jeffail/benthos/v3/lib/x/docs"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeRedisList] = TypeSpec{
		constructor: NewRedisList,
		Summary: `
Pushes messages onto the end of a Redis list (which is created if it doesn't
already exist) using the RPUSH command.`,
		Async: true,
		FieldSpecs: docs.FieldSpecs{
			docs.FieldCommon("url",
				"The URL of a Redis server to connect to. Database is optional and is supplied as the URL path.",
				"tcp://localhost:6379", "tcp://localhost:6379/1",
			),
			docs.FieldCommon("key", "The key of a Redis list."),
			docs.FieldCommon("max_in_flight", "The maximum number of messages to have in flight at a given time. Increase this to improve throughput."),
		},
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
