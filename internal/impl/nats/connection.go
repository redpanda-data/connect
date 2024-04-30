package nats

import (
	"context"
	"crypto/tls"
	"strings"

	"github.com/nats-io/nats.go"

	"github.com/benthosdev/benthos/v4/public/service"
)

// I've split the connection fields into two, which allows us to put tls and
// auth further down the fields stack. This is literally just polish for the
// docs.
func connectionHeadFields() []*service.ConfigField {
	return []*service.ConfigField{
		service.NewStringListField("urls").
			Description("A list of URLs to connect to. If an item of the list contains commas it will be expanded into multiple URLs.").
			Example([]string{"nats://127.0.0.1:4222"}).
			Example([]string{"nats://username:password@127.0.0.1:4222"}),
		service.NewStringField("name").
			Description("An optional name to assign to the connection. If not set, will default to the label").
			Default(""),
	}
}

func connectionTailFields() []*service.ConfigField {
	return []*service.ConfigField{
		service.NewTLSToggledField("tls"),
		authFieldSpec(),
	}
}

type connectionDetails struct {
	Urls     string
	Opts     []nats.Option
	authConf authConfig
}

func connectionDetailsFromParsed(conf *service.ParsedConfig, mgr *service.Resources, extraOpts ...nats.Option) (c connectionDetails, err error) {
	var urlList []string
	if urlList, err = conf.FieldStringList("urls"); err != nil {
		return
	}
	c.Urls = strings.Join(urlList, ",")

	var name string
	if name, err = conf.FieldString("name"); err != nil {
		return
	}
	if name == "" {
		name = mgr.Label()
	}
	c.Opts = append(c.Opts, nats.Name(name))

	var tlsEnabled bool
	var tlsConf *tls.Config
	if tlsConf, tlsEnabled, err = conf.FieldTLSToggled("tls"); err != nil {
		return
	}
	if tlsEnabled && tlsConf != nil {
		c.Opts = append(c.Opts, nats.Secure(tlsConf))
	}

	if c.authConf, err = AuthFromParsedConfig(conf.Namespace("auth")); err != nil {
		return
	}
	c.Opts = append(c.Opts, authConfToOptions(c.authConf, mgr.FS())...)

	c.Opts = append(c.Opts, errorHandlerOption(mgr.Logger()))
	c.Opts = append(c.Opts, extraOpts...)

	return
}

func (c *connectionDetails) get(_ context.Context) (*nats.Conn, error) {
	return nats.Connect(c.Urls, c.Opts...)
}
