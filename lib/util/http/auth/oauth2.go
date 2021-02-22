package auth

import (
	"context"
	"fmt"
	"sync"

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

type ROPCTokenSource struct {
	conf OAuth2Config
	mu   sync.RWMutex
	t    *oauth2.Token
}

func (s *ROPCTokenSource) Token() (*oauth2.Token, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.t != nil && s.t.Valid() {
		return s.t, nil
	}

	// refresh token
	conf := &oauth2.Config{
		ClientID:     s.conf.ClientKey,
		ClientSecret: s.conf.ClientSecret,
		Endpoint: oauth2.Endpoint{
			TokenURL: s.conf.TokenURL,
		},
		Scopes: s.conf.Scopes,
	}

	t, err := conf.PasswordCredentialsToken(context.Background(), s.conf.ROPC.Username, s.conf.ROPC.Password)
	if err != nil {
		return nil, fmt.Errorf("problem refreshing password credentials token: %s", err)
	}

	s.t = t
	return t, nil
}

//------------------------------------------------------------------------------

func (oauth OAuth2Config) GetTokenSource() oauth2.TokenSource {
	if !oauth.Enabled {
		return nil
	}
	ctx := context.Background()

	if oauth.ROPC.Enabled {
		rts := &ROPCTokenSource{
			conf: oauth,
			mu:   sync.RWMutex{},
		}
		return rts
	}

	conf := &clientcredentials.Config{
		ClientID:     oauth.ClientKey,
		ClientSecret: oauth.ClientSecret,
		TokenURL:     oauth.TokenURL,
		Scopes:       oauth.Scopes,
	}

	return conf.TokenSource(ctx)
}
