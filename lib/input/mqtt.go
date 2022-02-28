package input

import (
	"github.com/Jeffail/benthos/v3/internal/component/input"
	"github.com/Jeffail/benthos/v3/internal/docs"
	mqttconf "github.com/Jeffail/benthos/v3/internal/impl/mqtt"
	"github.com/Jeffail/benthos/v3/internal/interop"
	"github.com/Jeffail/benthos/v3/internal/tls"
	"github.com/Jeffail/benthos/v3/lib/input/reader"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeMQTT] = TypeSpec{
		constructor: fromSimpleConstructor(NewMQTT),
		Summary: `
Subscribe to topics on MQTT brokers.`,
		Description: `
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
[function interpolation](/docs/configuration/interpolation#metadata).`,
		FieldSpecs: docs.FieldSpecs{
			docs.FieldString("urls", "A list of URLs to connect to. If an item of the list contains commas it will be expanded into multiple URLs.").Array(),
			docs.FieldString("topics", "A list of topics to consume from.").Array(),
			docs.FieldString("client_id", "An identifier for the client connection."),
			docs.FieldString("dynamic_client_id_suffix", "Append a dynamically generated suffix to the specified `client_id` on each run of the pipeline. This can be useful when clustering Benthos producers.").Optional().Advanced().HasAnnotatedOptions(
				"nanoid", "append a nanoid of length 21 characters",
			),
			docs.FieldInt("qos", "The level of delivery guarantee to enforce.").HasOptions("0", "1", "2").Advanced(),
			docs.FieldBool("clean_session", "Set whether the connection is non-persistent.").Advanced(),
			mqttconf.WillFieldSpec(),
			docs.FieldString("connect_timeout", "The maximum amount of time to wait in order to establish a connection before the attempt is abandoned.", "1s", "500ms").HasDefault("30s").AtVersion("3.58.0"),
			docs.FieldString("user", "A username to assume for the connection.").Advanced(),
			docs.FieldString("password", "A password to provide for the connection.").Advanced(),
			docs.FieldInt("keepalive", "Max seconds of inactivity before a keepalive message is sent.").Advanced(),
			tls.FieldSpec().AtVersion("3.45.0"),
		},
		Categories: []Category{
			CategoryServices,
		},
	}
}

//------------------------------------------------------------------------------

// NewMQTT creates a new MQTT input type.
func NewMQTT(conf Config, mgr interop.Manager, log log.Modular, stats metrics.Type) (input.Streamed, error) {
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
