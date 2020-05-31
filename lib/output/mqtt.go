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
	Constructors[TypeMQTT] = TypeSpec{
		constructor: NewMQTT,
		Summary: `
Pushes messages to an MQTT broker.`,
		Description: `
The ` + "`topic`" + ` field can be dynamically set using function interpolations
described [here](/docs/configuration/interpolation#bloblang-queries). When sending batched
messages these interpolations are performed per message part.`,
		Async: true,
		FieldSpecs: docs.FieldSpecs{
			docs.FieldCommon("urls", "A list of URLs to connect to. If an item of the list contains commas it will be expanded into multiple URLs.", []string{"tcp://localhost:1883"}),
			docs.FieldCommon("qos", "The QoS value to set for each message.").HasOptions("0", "1", "2"),
			docs.FieldCommon("topic", "The topic to publish messages to."),
			docs.FieldCommon("client_id", "An identifier for the client."),
			docs.FieldAdvanced("user", "A username to connect with."),
			docs.FieldAdvanced("password", "A password to connect with."),
			docs.FieldCommon("max_in_flight", "The maximum number of messages to have in flight at a given time. Increase this to improve throughput."),
		},
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
