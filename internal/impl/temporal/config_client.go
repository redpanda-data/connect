package temporal

import (
	"fmt"

	"github.com/benthosdev/benthos/v4/public/service"
	"go.temporal.io/sdk/client"
)

func clientOptionsFields() []*service.ConfigField {
	return []*service.ConfigField{
		service.NewStringField("host").
			Description("The temporal server host."),
		service.NewIntField("port").
			Description("The temporal server port."),
		service.NewStringField("namespace").
			Description("The namespace name for this client to work with."),
		service.NewStringField("identity").
			Description("Sets an identify that can be used to track this host for debugging.").Optional(),
	}
}

func newClientOptionsFromParsed(conf *service.ParsedConfig) (*client.Options, error) {
	config := &client.Options{}

	host, err := conf.FieldString("host")
	if err != nil {
		return nil, err
	}

	port, err := conf.FieldInt("port")
	if err != nil {
		return nil, err
	}

	config.HostPort = fmt.Sprintf("%s:%d", host, port)

	if config.Namespace, err = conf.FieldString("namespace"); err != nil {
		return nil, err
	}

	if conf.Contains("identity") {
		if config.Identity, err = conf.FieldString("identity"); err != nil {
			return nil, err
		}
	}

	return config, nil
}
