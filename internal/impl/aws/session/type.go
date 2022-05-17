package session

// CredentialsConfig contains configuration params for AWS credentials.
type CredentialsConfig struct {
	Profile     string `json:"profile" yaml:"profile"`
	ID          string `json:"id" yaml:"id"`
	Secret      string `json:"secret" yaml:"secret"`
	Token       string `json:"token" yaml:"token"`
	UseEC2Creds bool   `json:"from_ec2_role" yaml:"from_ec2_role"`
	Role        string `json:"role" yaml:"role"`
	ExternalID  string `json:"role_external_id" yaml:"role_external_id"`
}

// Config contains configuration fields for an AWS session. This config is
// common across any AWS components.
type Config struct {
	Credentials CredentialsConfig `json:"credentials" yaml:"credentials"`
	Endpoint    string            `json:"endpoint" yaml:"endpoint"`
	Region      string            `json:"region" yaml:"region"`
}

// NewConfig returns a Config with default values.
func NewConfig() Config {
	return Config{
		Credentials: CredentialsConfig{
			Profile:    "",
			ID:         "",
			Secret:     "",
			Token:      "",
			Role:       "",
			ExternalID: "",
		},
		Endpoint: "",
		Region:   "",
	}
}
