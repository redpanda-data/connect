package pulsar

import (
	"errors"

	"github.com/benthosdev/benthos/v4/public/service"
)

func authField() *service.ConfigField {
	return service.NewObjectField("auth",
		service.NewObjectField("oauth2",
			service.NewBoolField("enabled").
				Description("Whether OAuth2 is enabled.").
				Default(false),
			service.NewStringField("audience").
				Description("OAuth2 audience.").
				Default(""),
			service.NewURLField("issuer_url").
				Description("OAuth2 issuer URL.").
				Default(""),
			service.NewStringField("private_key_file").
				Description("The path to a file containing a private key.").
				Default(""),
		).Description("Parameters for Pulsar OAuth2 authentication.").
			Optional(),
		service.NewObjectField("token",
			service.NewBoolField("enabled").
				Description("Whether Token Auth is enabled.").
				Default(false),
			service.NewStringField("token").
				Description("Actual base64 encoded token.").
				Default(""),
		).Description("Parameters for Pulsar Token authentication.").
			Optional(),
	).Description("Optional configuration of Pulsar authentication methods.").
		Version("3.60.0").
		Advanced().
		Optional()
}

type authConfig struct {
	OAuth2 oAuth2Config
	Token  tokenConfig
}

type oAuth2Config struct {
	Enabled        bool
	Audience       string
	IssuerURL      string
	PrivateKeyFile string
}

type tokenConfig struct {
	Enabled bool
	Token   string
}

func authFromParsed(p *service.ParsedConfig) (c authConfig, err error) {
	if !p.Contains("auth") {
		return
	}
	p = p.Namespace("auth")

	if p.Contains("oauth") {
		if c.OAuth2.Enabled, err = p.FieldBool("oauth", "enabled"); err != nil {
			return
		}
		if c.OAuth2.Audience, err = p.FieldString("oauth", "audience"); err != nil {
			return
		}
		if c.OAuth2.IssuerURL, err = p.FieldString("oauth", "issuer_url"); err != nil {
			return
		}
		if c.OAuth2.PrivateKeyFile, err = p.FieldString("oauth", "private_key_file"); err != nil {
			return
		}
	}

	if p.Contains("token") {
		if c.Token.Enabled, err = p.FieldBool("token", "enabled"); err != nil {
			return
		}
		if c.Token.Token, err = p.FieldString("token", "token"); err != nil {
			return
		}
	}
	return
}

// Validate checks whether Config is valid.
func (c *authConfig) Validate() error {
	if c.OAuth2.Enabled && c.Token.Enabled {
		return errors.New("only one auth method can be enabled at once")
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
func (c *oAuth2Config) Validate() error {
	if c.Audience == "" {
		return errors.New("oauth2 audience is empty")
	}
	if c.IssuerURL == "" {
		return errors.New("oauth2 issuer URL is empty")
	}
	if c.PrivateKeyFile == "" {
		return errors.New("oauth2 private key file is empty")
	}
	return nil
}

// ToMap returns OAuth2Config as a map representing OAuth2 client credentials.
func (c *oAuth2Config) ToMap() map[string]string {
	// Pulsar docs: https://pulsar.apache.org/docs/en/2.8.0/security-oauth2/#go-client
	return map[string]string{
		"type":       "client_credentials",
		"issuerUrl":  c.IssuerURL,
		"audience":   c.Audience,
		"privateKey": c.PrivateKeyFile,
	}
}

// Validate checks whether TokenConfig is valid.
func (c *tokenConfig) Validate() error {
	if c.Token == "" {
		return errors.New("token is empty")
	}
	return nil
}
