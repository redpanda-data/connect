package output

import (
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/output/writer"
	"github.com/Jeffail/benthos/v3/lib/types"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeCassandra] = TypeSpec{
		constructor: NewCassandra,
		Description: `
Sends messages to a Cassandra database.`,
	}
}

//------------------------------------------------------------------------------

// NewCassandra creates a new Cassandra output type.
func NewCassandra(conf Config, mgr types.Manager, log log.Modular, stats metrics.Type) (Type, error) {
	c, err := writer.NewCassandra(conf.Cassandra, log, stats)
	if err != nil {
		return nil, err
	}
	return NewWriter(
		"cassandra", c, log, stats,
	)
}

//------------------------------------------------------------------------------
