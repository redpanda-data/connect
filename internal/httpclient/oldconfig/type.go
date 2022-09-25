package oldconfig

import "net/http"

// AuthConfig contains configuration params for various HTTP auth strategies.
type AuthConfig struct {
	OAuth     OAuthConfig     `json:"oauth" yaml:"oauth"`
	BasicAuth BasicAuthConfig `json:"basic_auth" yaml:"basic_auth"`
	JWT       JWTConfig       `json:"jwt" yaml:"jwt"`
}

// NewAuthConfig creates a new Config with default values.
func NewAuthConfig() AuthConfig {
	return AuthConfig{
		OAuth:     NewOAuthConfig(),
		BasicAuth: NewBasicAuthConfig(),
		JWT:       NewJWTConfig(),
	}
}

// Sign method to sign an HTTP request for configured auth strategies.
func (c AuthConfig) Sign(req *http.Request) error {
	if err := c.OAuth.Sign(req); err != nil {
		return err
	}
	if err := c.JWT.Sign(req); err != nil {
		return err
	}
	return c.BasicAuth.Sign(req)
}
