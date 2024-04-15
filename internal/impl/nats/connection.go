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
	}
}

func connectionTailFields() []*service.ConfigField {
	return []*service.ConfigField{
		service.NewTLSToggledField("tls"),
		service.NewInternalField(authFieldSpec()),
	}
}

type connectionDetails struct {
	label    string
	logger   *service.Logger
	tlsConf  *tls.Config
	authConf authConfig
	fs       *service.FS
	urls     string
}

func connectionDetailsFromParsed(conf *service.ParsedConfig, mgr *service.Resources) (c connectionDetails, err error) {
	c.label = mgr.Label()
	c.fs = mgr.FS()
	c.logger = mgr.Logger()

	var urlList []string
	if urlList, err = conf.FieldStringList("urls"); err != nil {
		return
	}
	c.urls = strings.Join(urlList, ",")

	var tlsEnabled bool
	if c.tlsConf, tlsEnabled, err = conf.FieldTLSToggled("tls"); err != nil {
		return
	}
	if !tlsEnabled {
		c.tlsConf = nil
	}

	if c.authConf, err = AuthFromParsedConfig(conf.Namespace("auth")); err != nil {
		return
	}
	return
}

func (c *connectionDetails) get(_ context.Context, extraOpts ...nats.Option) (*nats.Conn, error) {
	var opts []nats.Option
	if c.tlsConf != nil {
		opts = append(opts, nats.Secure(c.tlsConf))
	}
	opts = append(opts, nats.Name(c.label))
	opts = append(opts, errorHandlerOption(c.logger))
	opts = append(opts, authConfToOptions(c.authConf, c.fs)...)
	opts = append(opts, extraOpts...)
	return nats.Connect(c.urls, opts...)
}
