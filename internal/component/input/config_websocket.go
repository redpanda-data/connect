package input

import (
	"github.com/benthosdev/benthos/v4/internal/httpclient/oldconfig"
	btls "github.com/benthosdev/benthos/v4/internal/tls"
)

// OpenMsgType represents the type of the open_message field.
type OpenMsgType string

const (
	// OpenMsgTypeBinary sets the type of open_message to binary.
	OpenMsgTypeBinary OpenMsgType = "binary"
	// OpenMsgTypeText sets the type of open_message to text (UTF-8 encoded text data).
	OpenMsgTypeText OpenMsgType = "text"
)

// WebsocketConfig contains configuration fields for the Websocket input type.
type WebsocketConfig struct {
	URL                  string      `json:"url" yaml:"url"`
	OpenMsg              string      `json:"open_message" yaml:"open_message"`
	OpenMsgType          OpenMsgType `json:"open_message_type" yaml:"open_message_type"`
	oldconfig.AuthConfig `json:",inline" yaml:",inline"`
	TLS                  btls.Config `json:"tls" yaml:"tls"`
}

// NewWebsocketConfig creates a new WebsocketConfig with default values.
func NewWebsocketConfig() WebsocketConfig {
	return WebsocketConfig{
		URL:         "",
		OpenMsg:     "",
		OpenMsgType: OpenMsgTypeBinary,
		AuthConfig:  oldconfig.NewAuthConfig(),
		TLS:         btls.NewConfig(),
	}
}
