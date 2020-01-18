package input

import (
	"github.com/Jeffail/benthos/v3/lib/input/reader"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeNATS] = TypeSpec{
		constructor: NewNATS,
		Description: `
Subscribe to a NATS subject. NATS is at-most-once, if you need at-least-once
behaviour then look at NATS Stream.

The urls can contain username/password semantics. e.g.
nats://derek:pass@localhost:4222

### Metadata

This input adds the following metadata fields to each message:

` + "``` text" + `
- nats_subject
` + "```" + `

You can access these metadata fields using
[function interpolation](/docs/configuration/interpolation#metadata).`,
	}
}

//------------------------------------------------------------------------------

// NewNATS creates a new NATS input type.
func NewNATS(conf Config, mgr types.Manager, log log.Modular, stats metrics.Type) (Type, error) {
	n, err := reader.NewNATS(conf.NATS, log, stats)
	if err != nil {
		return nil, err
	}
	return NewAsyncReader(TypeNATS, true, reader.NewAsyncPreserver(n), log, stats)
}

//------------------------------------------------------------------------------
