package nats

import (
	"github.com/nats-io/nats.go"

	"github.com/benthosdev/benthos/v4/internal/impl/nats/auth"
	"github.com/benthosdev/benthos/v4/public/service"
)

func authConfToOptions(auth auth.Config) []nats.Option {
	var opts []nats.Option
	if auth.NKeyFile != "" {
		if opt, err := nats.NkeyOptionFromSeed(auth.NKeyFile); err != nil {
			opts = append(opts, func(*nats.Options) error { return err })
		} else {
			opts = append(opts, opt)
		}
	}

	if auth.UserCredentialsFile != "" {
		opts = append(opts, nats.UserCredentials(auth.UserCredentialsFile))
	}
	return opts
}

// AuthFromParsedConfig attempts to extract an auth config from a ParsedConfig.
func AuthFromParsedConfig(p *service.ParsedConfig) (c auth.Config, err error) {
	c = auth.New()
	if p.Contains("nkey_file") {
		if c.NKeyFile, err = p.FieldString("nkey_file"); err != nil {
			return
		}
	}
	if p.Contains("user_credentials_file") {
		if c.UserCredentialsFile, err = p.FieldString("user_credentials_file"); err != nil {
			return
		}
	}
	return
}
