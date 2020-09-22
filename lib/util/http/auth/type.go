package auth

import (
	"net/http"
)

// Config contains configuration params for various HTTP auth strategies.
type Config struct {
	OAuth     OAuthConfig     `json:"oauth" yaml:"oauth"`
	BasicAuth BasicAuthConfig `json:"basic_auth" yaml:"basic_auth"`
}

// NewConfig creates a new Config with default values.
func NewConfig() Config {
	return Config{
		OAuth:     NewOAuthConfig(),
		BasicAuth: NewBasicAuthConfig(),
	}
}

// Sign method to sign an HTTP request for configured auth strategies.
func (c Config) Sign(req *http.Request) error {
	if err := c.OAuth.Sign(req); err != nil {
		return err
	}
	return c.BasicAuth.Sign(req)
}

//------------------------------------------------------------------------------

// WebsocketConfig contains configuration params for various HTTP auth
// strategies suitable for websockets.
type WebsocketConfig struct {
	OAuth     OAuthConfig     `json:"oauth" yaml:"oauth"`
	BasicAuth BasicAuthConfig `json:"basic_auth" yaml:"basic_auth"`
}

// NewWebsocketConfig creates a new WebsocketConfig with default values.
func NewWebsocketConfig() WebsocketConfig {
	return WebsocketConfig{
		OAuth:     NewOAuthConfig(),
		BasicAuth: NewBasicAuthConfig(),
	}
}

// Sign method to sign an HTTP request for configured auth strategies.
func (c WebsocketConfig) Sign(req *http.Request) error {
	if err := c.OAuth.Sign(req); err != nil {
		return err
	}
	return c.BasicAuth.Sign(req)
}
