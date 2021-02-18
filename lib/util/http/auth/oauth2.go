package auth

import (
	"context"
	"fmt"
	"net/http"

	"golang.org/x/oauth2"
	"golang.org/x/oauth2/clientcredentials"
)

//------------------------------------------------------------------------------

// OAuth2Config holds the configuration parameters for an OAuth2 exchange.
type OAuth2Config struct {
	Enabled      bool     `json:"enabled" yaml:"enabled"`
	ClientKey    string   `json:"client_key" yaml:"client_key"`
	ClientSecret string   `json:"client_secret" yaml:"client_secret"`
	TokenURL     string   `json:"token_url" yaml:"token_url"`
	Scopes       []string `json:"scopes" yaml:"scopes"`
	ROPC         ROPC     `json:"ropc" yaml:"ropc"`
}

// ROPC holds config parameters specific to the resource owners password credentials grant
// specific details at https://tools.ietf.org/html/rfc6749#section-4.3
type ROPC struct {
	Enabled   bool   `json:"enabled" yaml:"enabled"`
	Username  string `json:"username" yaml:"username"`
	Password  string `json:"password" yaml:"password"`
	TokenType string `json:"token_type" yaml:"token_type"`
}

// NewOAuth2Config returns a new OAuth2Config with default values.
func NewOAuth2Config() OAuth2Config {
	return OAuth2Config{
		Enabled:      false,
		ClientKey:    "",
		ClientSecret: "",
		TokenURL:     "",
		Scopes:       []string{},
		ROPC: ROPC{
			Enabled:   false,
			Username:  "",
			Password:  "",
			TokenType: "",
		},
	}
}

//------------------------------------------------------------------------------

// Client returns an http.Client with OAuth2 configured.
func (oauth OAuth2Config) Client(ctx context.Context) *http.Client {
	if !oauth.Enabled {
		var client http.Client
		return &client
	}

	if oauth.ROPC.Enabled {
		conf := &oauth2.Config{
			ClientID:     oauth.ClientKey,
			ClientSecret: oauth.ClientSecret,
			Endpoint: oauth2.Endpoint{
				TokenURL: oauth.TokenURL,
			},
			Scopes: oauth.Scopes,
		}
		ctx := context.Background()
		token, err := conf.PasswordCredentialsToken(ctx, oauth.ROPC.Username, oauth.ROPC.Password)
		if err != nil {
			//hmmm this probably needs to go elsewhere
			fmt.Println("problem with PasswordCredentialsToken: " + err.Error())
		}
		token.TokenType = oauth.ROPC.TokenType
		return conf.Client(ctx, token)
	}

	conf := &clientcredentials.Config{
		ClientID:     oauth.ClientKey,
		ClientSecret: oauth.ClientSecret,
		TokenURL:     oauth.TokenURL,
		Scopes:       oauth.Scopes,
	}
	return conf.Client(ctx)
}
