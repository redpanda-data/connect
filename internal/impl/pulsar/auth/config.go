package auth

import "fmt"

// Config contains configuration params for Pulsar authentication.
type Config struct {
	OAuth2 OAuth2Config `json:"oauth2" yaml:"oauth2"`
	Token  TokenConfig  `json:"token" yaml:"token"`
}

// OAuth2Config contains configuration params for Pulsar OAuth2 authentication.
type OAuth2Config struct {
	Enabled        bool   `json:"enabled" yaml:"enabled"`
	Audience       string `json:"audience" yaml:"audience"`
	IssuerURL      string `json:"issuer_url" yaml:"issuer_url"`
	PrivateKeyFile string `json:"private_key_file" yaml:"private_key_file"`
}

// TokenConfig contains configuration params for Pulsar Token authentication.
type TokenConfig struct {
	Enabled bool   `json:"enabled" yaml:"enabled"`
	Token   string `json:"token" yaml:"token"`
}

// New creates a new Config instance.
func New() Config {
	return Config{
		OAuth2: NewOAuth(),
		Token:  NewToken(),
	}
}

// NewOAuth creates a new OAuth2Config instance.
func NewOAuth() OAuth2Config {
	return OAuth2Config{
		Enabled:        false,
		PrivateKeyFile: "",
		Audience:       "",
		IssuerURL:      "",
	}
}

// NewToken creates a new TokenConfig instance.
func NewToken() TokenConfig {
	return TokenConfig{
		Enabled: false,
		Token:   "",
	}
}

// Validate checks whether Config is valid.
func (c *Config) Validate() error {
	if c.OAuth2.Enabled && c.Token.Enabled {
		return fmt.Errorf("only one auth method can be enabled at once")
	}
	if c.OAuth2.Enabled {
		return c.OAuth2.Validate()
	}
	if c.Token.Enabled {
		return c.Token.Validate()
	}
	return nil
}

// Validate checks whether OAuth2Config is valid.
func (c *OAuth2Config) Validate() error {
	if c.Audience == "" {
		return fmt.Errorf("oauth2 audience is empty")
	}
	if c.IssuerURL == "" {
		return fmt.Errorf("oauth2 issuer URL is empty")
	}
	if c.PrivateKeyFile == "" {
		return fmt.Errorf("oauth2 private key file is empty")
	}
	return nil
}

// Validate checks whether TokenConfig is valid.
func (c *TokenConfig) Validate() error {
	if c.Token == "" {
		return fmt.Errorf("token is empty")
	}
	return nil
}
