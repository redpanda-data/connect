package output

import (
	"github.com/benthosdev/benthos/v4/internal/http/docs/auth"
	btls "github.com/benthosdev/benthos/v4/internal/tls"
)

// WebsocketConfig contains configuration fields for the Websocket output type.
type WebsocketConfig struct {
	URL         string `json:"url" yaml:"url"`
	auth.Config `json:",inline" yaml:",inline"`
	TLS         btls.Config `json:"tls" yaml:"tls"`
}

// NewWebsocketConfig creates a new WebsocketConfig with default values.
func NewWebsocketConfig() WebsocketConfig {
	return WebsocketConfig{
		URL:    "",
		Config: auth.NewConfig(),
		TLS:    btls.NewConfig(),
	}
}
