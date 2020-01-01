package output

import (
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/output/writer"
	"github.com/Jeffail/benthos/v3/lib/types"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeMQTT] = TypeSpec{
		constructor: NewMQTT,
		Description: `
Pushes messages to an MQTT broker.

The ` + "`topic`" + ` field can be dynamically set using function interpolations
described [here](../config_interpolation.md#functions). When sending batched
messages these interpolations are performed per message part.`,
		Async: true,
	}
}

//------------------------------------------------------------------------------

// NewMQTT creates a new MQTT output type.
func NewMQTT(conf Config, mgr types.Manager, log log.Modular, stats metrics.Type) (Type, error) {
	w, err := writer.NewMQTT(conf.MQTT, log, stats)
	if err != nil {
		return nil, err
	}
	if conf.MQTT.MaxInFlight == 1 {
		return NewWriter(TypeMQTT, w, log, stats)
	}
	return NewAsyncWriter(TypeMQTT, conf.MQTT.MaxInFlight, w, log, stats)
}

//------------------------------------------------------------------------------
