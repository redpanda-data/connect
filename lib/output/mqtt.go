package output

import (
	"github.com/Jeffail/benthos/v3/internal/component/output"
	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/internal/mqttconf"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/output/writer"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/Jeffail/benthos/v3/lib/util/tls"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeMQTT] = TypeSpec{
		constructor: fromSimpleConstructor(NewMQTT),
		Summary: `
Pushes messages to an MQTT broker.`,
		Description: `
The ` + "`topic`" + ` field can be dynamically set using function interpolations
described [here](/docs/configuration/interpolation#bloblang-queries). When sending batched
messages these interpolations are performed per message part.`,
		Async: true,
		FieldSpecs: docs.FieldSpecs{
			docs.FieldCommon("urls", "A list of URLs to connect to. If an item of the list contains commas it will be expanded into multiple URLs.", []string{"tcp://localhost:1883"}).Array(),
			docs.FieldCommon("topic", "The topic to publish messages to."),
			docs.FieldCommon("client_id", "An identifier for the client connection."),
			docs.FieldString("dynamic_client_id_suffix", "Append a dynamically generated suffix to the specified `client_id` on each run of the pipeline. This can be useful when clustering Benthos producers.").Optional().Advanced().HasAnnotatedOptions(
				"nanoid", "append a nanoid of length 21 characters",
			),
			docs.FieldCommon("qos", "The QoS value to set for each message.").HasOptions("0", "1", "2"),
			docs.FieldString("connect_timeout", "The maximum amount of time to wait in order to establish a connection before the attempt is abandoned.", "1s", "500ms").HasDefault("30s").AtVersion("3.58.0"),
			docs.FieldString("write_timeout", "The maximum amount of time to wait to write data before the attempt is abandoned.", "1s", "500ms").HasDefault("3s").AtVersion("3.58.0"),
			docs.FieldBool("retained", "Set message as retained on the topic."),
			docs.FieldString("retained_interpolated", "Override the value of `retained` with an interpolable value, this allows it to be dynamically set based on message contents. The value must resolve to either `true` or `false`.").IsInterpolated().Advanced().AtVersion("3.59.0"),
			mqttconf.WillFieldSpec(),
			docs.FieldAdvanced("user", "A username to connect with."),
			docs.FieldAdvanced("password", "A password to connect with."),
			docs.FieldAdvanced("keepalive", "Max seconds of inactivity before a keepalive message is sent."),
			tls.FieldSpec().AtVersion("3.45.0"),
			docs.FieldCommon("max_in_flight", "The maximum number of messages to have in flight at a given time. Increase this to improve throughput."),
		},
		Categories: []Category{
			CategoryServices,
		},
	}
}

//------------------------------------------------------------------------------

// NewMQTT creates a new MQTT output type.
func NewMQTT(conf Config, mgr types.Manager, log log.Modular, stats metrics.Type) (output.Streamed, error) {
	w, err := writer.NewMQTTV2(conf.MQTT, mgr, log, stats)
	if err != nil {
		return nil, err
	}
	a, err := NewAsyncWriter(TypeMQTT, conf.MQTT.MaxInFlight, w, log, stats)
	if err != nil {
		return nil, err
	}
	return OnlySinglePayloads(a), nil
}

//------------------------------------------------------------------------------
