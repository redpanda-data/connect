package oldconfig

import "net/http"

// BasicAuthConfig contains fields for setting basic authentication in HTTP
// requests.
type BasicAuthConfig struct {
	Enabled  bool   `json:"enabled" yaml:"enabled"`
	Username string `json:"username" yaml:"username"`
	Password string `json:"password" yaml:"password"`
}

// NewBasicAuthConfig returns a default configuration for basic authentication
// in HTTP client requests.
func NewBasicAuthConfig() BasicAuthConfig {
	return BasicAuthConfig{
		Enabled:  false,
		Username: "",
		Password: "",
	}
}

//------------------------------------------------------------------------------

// Sign method to sign an HTTP request for an OAuth exchange.
func (basic BasicAuthConfig) Sign(req *http.Request) error {
	if basic.Enabled {
		req.SetBasicAuth(basic.Username, basic.Password)
	}
	return nil
}
