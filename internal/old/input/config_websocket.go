package input

import (
	"github.com/benthosdev/benthos/v4/internal/http/docs/auth"
	btls "github.com/benthosdev/benthos/v4/internal/tls"
)

// WebsocketConfig contains configuration fields for the Websocket input type.
type WebsocketConfig struct {
	URL         string `json:"url" yaml:"url"`
	OpenMsg     string `json:"open_message" yaml:"open_message"`
	auth.Config `json:",inline" yaml:",inline"`
	TLS         btls.Config `json:"tls" yaml:"tls"`
}

// NewWebsocketConfig creates a new WebsocketConfig with default values.
func NewWebsocketConfig() WebsocketConfig {
	return WebsocketConfig{
		URL:     "",
		OpenMsg: "",
		Config:  auth.NewConfig(),
		TLS:     btls.NewConfig(),
	}
}
