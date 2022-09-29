package auth

// Config contains configuration params for NATS authentication.
type Config struct {
	NKeyFile            string `json:"nkey_file" yaml:"nkey_file"`
	UserCredentialsFile string `json:"user_credentials_file" yaml:"user_credentials_file"`
}

// New creates a new Config instance.
func New() Config {
	return Config{
		NKeyFile:            "",
		UserCredentialsFile: "",
	}
}
