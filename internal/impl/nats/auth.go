package nats

import (
	"github.com/Jeffail/benthos/v3/internal/impl/nats/auth"
	"github.com/Jeffail/benthos/v3/public/service"
)

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
