package output

import (
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/output/writer"
	"github.com/Jeffail/benthos/v3/lib/types"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeAMQP] = TypeSpec{
		constructor: NewAMQP,
		Description: `
DEPRECATED: This output is deprecated and scheduled for removal in Benthos V4.
Please use [` + "`amqp_0_9`" + `](amqp_0_9) instead.`,
		Deprecated: true,
	}
}

//------------------------------------------------------------------------------

// NewAMQP creates a new AMQP output type.
// TODO: V4 Remove this.
func NewAMQP(conf Config, mgr types.Manager, log log.Modular, stats metrics.Type) (Type, error) {
	log.Warnln("The amqp input is deprecated, please use amqp_0_9 instead.")
	a, err := writer.NewAMQP(conf.AMQP, log, stats)
	if err != nil {
		return nil, err
	}
	return NewWriter(
		"amqp", a, log, stats,
	)
}

//------------------------------------------------------------------------------
