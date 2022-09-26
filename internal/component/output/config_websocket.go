package output

import (
	"github.com/benthosdev/benthos/v4/internal/httpclient/oldconfig"
	btls "github.com/benthosdev/benthos/v4/internal/tls"
)

// WebsocketConfig contains configuration fields for the Websocket output type.
type WebsocketConfig struct {
	URL                  string `json:"url" yaml:"url"`
	oldconfig.AuthConfig `json:",inline" yaml:",inline"`
	TLS                  btls.Config `json:"tls" yaml:"tls"`
}

// NewWebsocketConfig creates a new WebsocketConfig with default values.
func NewWebsocketConfig() WebsocketConfig {
	return WebsocketConfig{
		URL:        "",
		AuthConfig: oldconfig.NewAuthConfig(),
		TLS:        btls.NewConfig(),
	}
}
