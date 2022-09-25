package input

import (
	"github.com/benthosdev/benthos/v4/internal/httpclient/oldconfig"
	btls "github.com/benthosdev/benthos/v4/internal/tls"
)

// WebsocketConfig contains configuration fields for the Websocket input type.
type WebsocketConfig struct {
	URL                  string `json:"url" yaml:"url"`
	OpenMsg              string `json:"open_message" yaml:"open_message"`
	oldconfig.AuthConfig `json:",inline" yaml:",inline"`
	TLS                  btls.Config `json:"tls" yaml:"tls"`
}

// NewWebsocketConfig creates a new WebsocketConfig with default values.
func NewWebsocketConfig() WebsocketConfig {
	return WebsocketConfig{
		URL:        "",
		OpenMsg:    "",
		AuthConfig: oldconfig.NewAuthConfig(),
		TLS:        btls.NewConfig(),
	}
}
