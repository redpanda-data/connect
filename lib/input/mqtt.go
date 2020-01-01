package input

import (
	"github.com/Jeffail/benthos/v3/lib/input/reader"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeMQTT] = TypeSpec{
		constructor: NewMQTT,
		Description: `
Subscribe to topics on MQTT brokers.

Messages consumed by this input can be processed in parallel, meaning a single
instance of this input can utilise any number of threads within a
` + "`pipeline`" + ` section of a config.

### Metadata

This input adds the following metadata fields to each message:

` + "``` text" + `
- mqtt_duplicate
- mqtt_qos
- mqtt_retained
- mqtt_topic
- mqtt_message_id
` + "```" + `

You can access these metadata fields using
[function interpolation](../config_interpolation.md#metadata).`,
	}
}

//------------------------------------------------------------------------------

// NewMQTT creates a new MQTT input type.
func NewMQTT(conf Config, mgr types.Manager, log log.Modular, stats metrics.Type) (Type, error) {
	m, err := reader.NewMQTT(conf.MQTT, log, stats)
	if err != nil {
		return nil, err
	}
	return NewAsyncReader(
		TypeMQTT,
		true,
		reader.NewAsyncPreserver(m),
		log, stats,
	)
}

//------------------------------------------------------------------------------
