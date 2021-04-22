package input

import (
	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/lib/input/reader"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/Jeffail/benthos/v3/lib/util/tls"
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
			docs.FieldCommon("urls", "A list of URLs to connect to. If an item of the list contains commas it will be expanded into multiple URLs.").Array(),
			docs.FieldCommon("topics", "A list of topics to consume from.").Array(),
			docs.FieldCommon("client_id", "An identifier for the client connection."),
			docs.FieldAdvanced("qos", "The level of delivery guarantee to enforce.").HasOptions("0", "1", "2"),
			docs.FieldAdvanced("clean_session", "Set whether the connection is non-persistent."),
			docs.FieldAdvanced("user", "A username to assume for the connection."),
			docs.FieldAdvanced("password", "A password to provide for the connection."),
			tls.FieldSpec(),
			docs.FieldDeprecated("stale_connection_timeout"),
		},
		Categories: []Category{
			CategoryServices,
		},
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
