package auth

type Config struct {
	OAuth2 OAuth2Config `json:"oauth2" yaml:"oauth2"`
	Token  TokenConfig  `json:"token" yaml:"token"`
}

type OAuth2Config struct {
	Enabled    bool   `json:"enabled" yaml:"enabled"`
	ClientID   string `json:"client_id" yaml:"client_id"`
	PrivateKey string `json:"private_key" yaml:"private_key"`
	Audience   string `json:"audience" yaml:"audience"`
	IssuerURL  string `json:"issuer_url" yaml:"issuer_url"`
}

type TokenConfig struct {
	Enabled bool   `json:"enabled" yaml:"enabled"`
	Token   string `json:"token" yaml:"token"`
}

func New() Config {
	return Config{
		OAuth2: NewOAuth(),
		Token:  NewToken(),
	}
}

func NewOAuth() OAuth2Config {
	return OAuth2Config{
		Enabled:    false,
		ClientID:   "",
		PrivateKey: "",
		Audience:   "",
		IssuerURL:  "",
	}
}

func NewToken() TokenConfig {
	return TokenConfig{
		Enabled: false,
		Token:   "",
	}
}
