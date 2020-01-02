// +build ZMQ4

package input

import (
	"github.com/Jeffail/benthos/v3/lib/input/reader"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeZMQ4] = TypeSpec{
		constructor: NewZMQ4,
		Description: `
ZMQ4 is supported but currently depends on C bindings. Since this is an
annoyance when building or using Benthos it is not compiled by default.

Build it into your project by getting libzmq installed on your machine, then
build with the tag: 'go install -tags "ZMQ4" github.com/Jeffail/benthos/v3/cmd/...'

Messages consumed by this input can be processed in parallel, meaning a single
instance of this input can utilise any number of threads within a
` + "`pipeline`" + ` section of a config.

ZMQ4 input supports PULL and SUB sockets only. If there is demand for other
socket types then they can be added easily.`,
	}
}

//------------------------------------------------------------------------------

// NewZMQ4 creates a new ZMQ input type.
func NewZMQ4(conf Config, mgr types.Manager, log log.Modular, stats metrics.Type) (Type, error) {
	z, err := reader.NewZMQ4(conf.ZMQ4, log, stats)
	if err != nil {
		return nil, err
	}
	return NewAsyncReader(TypeZMQ4, true, reader.NewAsyncPreserver(z), log, stats)
}

//------------------------------------------------------------------------------
