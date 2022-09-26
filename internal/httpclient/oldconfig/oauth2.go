package oldconfig

import (
	"context"
	"net/http"

	"golang.org/x/oauth2/clientcredentials"
)

// OAuth2Config holds the configuration parameters for an OAuth2 exchange.
type OAuth2Config struct {
	Enabled      bool     `json:"enabled" yaml:"enabled"`
	ClientKey    string   `json:"client_key" yaml:"client_key"`
	ClientSecret string   `json:"client_secret" yaml:"client_secret"`
	TokenURL     string   `json:"token_url" yaml:"token_url"`
	Scopes       []string `json:"scopes" yaml:"scopes"`
}

// NewOAuth2Config returns a new OAuth2Config with default values.
func NewOAuth2Config() OAuth2Config {
	return OAuth2Config{
		Enabled:      false,
		ClientKey:    "",
		ClientSecret: "",
		TokenURL:     "",
		Scopes:       []string{},
	}
}

//------------------------------------------------------------------------------

// Client returns an http.Client with OAuth2 configured.
func (oauth OAuth2Config) Client(ctx context.Context) *http.Client {
	if !oauth.Enabled {
		var client http.Client
		return &client
	}

	conf := &clientcredentials.Config{
		ClientID:     oauth.ClientKey,
		ClientSecret: oauth.ClientSecret,
		TokenURL:     oauth.TokenURL,
		Scopes:       oauth.Scopes,
	}

	return conf.Client(ctx)
}
