package sasl

import (
	"github.com/benthosdev/benthos/v4/internal/docs"
)

// Config contains configuration for SASL based authentication.
type Config struct {
	Mechanism   string `json:"mechanism" yaml:"mechanism"`
	User        string `json:"user" yaml:"user"`
	Password    string `json:"password" yaml:"password"`
	AccessToken string `json:"access_token" yaml:"access_token"`
	TokenCache  string `json:"token_cache" yaml:"token_cache"`
	TokenKey    string `json:"token_key" yaml:"token_key"`
}

// NewConfig returns a new SASL config for Kafka with default values.
func NewConfig() Config {
	return Config{
		Mechanism: "none",
	}
}

// FieldSpec returns specs for SASL fields.
func FieldSpec() docs.FieldSpec {
	return docs.FieldObject("sasl", "Enables SASL authentication.").WithChildren(
		docs.FieldString("mechanism", "The SASL authentication mechanism, if left empty SASL authentication is not used. Warning: SCRAM based methods within Benthos have not received a security audit.").HasAnnotatedOptions(
			"none", "Default, no SASL authentication.",
			"PLAIN", "Plain text authentication. NOTE: When using plain text auth it is extremely likely that you'll also need to [enable TLS](#tlsenabled).",
			"OAUTHBEARER", "OAuth Bearer based authentication.",
			"SCRAM-SHA-256", "Authentication using the SCRAM-SHA-256 mechanism.",
			"SCRAM-SHA-512", "Authentication using the SCRAM-SHA-512 mechanism.",
		),
		docs.FieldString("user", "A PLAIN username. It is recommended that you use environment variables to populate this field.", "${USER}"),
		docs.FieldString("password", "A PLAIN password. It is recommended that you use environment variables to populate this field.", "${PASSWORD}").Secret(),
		docs.FieldString("access_token", "A static OAUTHBEARER access token"),
		docs.FieldString("token_cache", "Instead of using a static `access_token` allows you to query a [`cache`](/docs/components/caches/about) resource to fetch OAUTHBEARER tokens from"),
		docs.FieldString("token_key", "Required when using a `token_cache`, the key to query the cache with for tokens."),
	).Advanced()
}
