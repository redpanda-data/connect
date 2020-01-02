package output

import (
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/output/writer"
	"github.com/Jeffail/benthos/v3/lib/types"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeAMQP09] = TypeSpec{
		constructor: NewAMQP09,
		Description: `
Sends messages to an AMQP (0.91) exchange. AMQP is a messaging protocol used by
various message brokers, including RabbitMQ. The metadata from each message are
delivered as headers.

It's possible for this output type to create the target exchange by setting
` + "`exchange_declare.enabled` to `true`" + `, if the exchange already exists
then the declaration passively verifies that the settings match.

Exchange type options are: direct|fanout|topic|x-custom

TLS is automatic when connecting to an ` + "`amqps`" + ` URL, but custom
settings can be enabled in the ` + "`tls`" + ` section.

The field 'key' can be dynamically set using function interpolations described
[here](../config_interpolation.md#functions).`,
		Async: true,
	}
}

//------------------------------------------------------------------------------

// NewAMQP09 creates a new AMQP output type.
func NewAMQP09(conf Config, mgr types.Manager, log log.Modular, stats metrics.Type) (Type, error) {
	a, err := writer.NewAMQP(conf.AMQP09, log, stats)
	if err != nil {
		return nil, err
	}
	if conf.AMQP09.MaxInFlight == 1 {
		return NewWriter(TypeAMQP09, a, log, stats)
	}
	return NewAsyncWriter(
		TypeAMQP09, conf.AMQP09.MaxInFlight, a, log, stats,
	)
}

//------------------------------------------------------------------------------
