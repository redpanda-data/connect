package input

import (
	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/lib/input/reader"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeAMQP] = TypeSpec{
		constructor: fromSimpleConstructor(NewAMQP),
		Description: `
DEPRECATED: This input is deprecated and scheduled for removal in Benthos V4.
Please use [` + "`amqp_0_9`" + `](amqp_0_9) instead.`,
		Status: docs.StatusDeprecated,
	}
}

//------------------------------------------------------------------------------

// NewAMQP creates a new AMQP input type.
// TODO: V4 Remove this.
func NewAMQP(conf Config, mgr types.Manager, log log.Modular, stats metrics.Type) (Type, error) {
	log.Warnln("The amqp input is deprecated, please use amqp_0_9 instead.")
	a, err := reader.NewAMQP(conf.AMQP, log, stats)
	if err != nil {
		return nil, err
	}
	return NewReader("amqp", a, log, stats)
}

//------------------------------------------------------------------------------
