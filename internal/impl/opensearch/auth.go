package opensearch

import (
	"context"
	"errors"
	"fmt"

	"github.com/benthosdev/benthos/v4/public/service"
	"golang.org/x/oauth2"
)

const (
	aFieldOAuth = "oauth2"

	abFieldEnabled = "enabled"

	aFieldAuthStaticAccessToken = "access_token"
	aFieldAuthTokenCache        = "token_cache"
	aFieldAuthTokenKey          = "token_key"
)

// OAuthAuthField returns a config field spec for basic authentication.
func OAuthAuthField() *service.ConfigField {
	return service.NewObjectField(aFieldOAuth,
		service.NewBoolField(abFieldEnabled).
			Description("Whether to use OAuth2 authentication.").
			Advanced().
			Default(false),
		service.NewStringField(aFieldAuthStaticAccessToken).
			Description("A static access token to use for authentication.").
			Advanced().
			Secret().
			Default(""),
		service.NewStringField(aFieldAuthTokenCache).
			Description("Instead of using a static `access_token` allows you to query a [`cache`](/docs/components/caches/about) resource to fetch tokens from.").
			Advanced().
			Default(""),
		service.NewStringField(aFieldAuthTokenKey).
			Description("Required when using a `token_cache`, the key to query the cache with for tokens.").
			Advanced().
			Default(""),
	).Description("Allows you to specify OAuth2 authentication.").
		Advanced().
		Optional()
}

func oAuthFromParsed(conf *service.ParsedConfig) (res OAuthConfig, err error) {
	res = NewOAuth2Config()
	if !conf.Contains(aFieldOAuth) {
		return
	}
	conf = conf.Namespace(aFieldOAuth)
	if res.Enabled, err = conf.FieldBool(abFieldEnabled); err != nil {
		return
	}

	staticToken, err := conf.FieldString(aFieldAuthStaticAccessToken)
	if err != nil && staticToken != "" {
		if res.StaticAccessToken, err = conf.FieldString(aFieldAuthStaticAccessToken); err != nil {
			return
		}
	} else {
		if res.TokenCacheName, err = conf.FieldString(aFieldAuthTokenCache); err != nil {
			return
		}
		if res.TokenCacheKey, err = conf.FieldString(aFieldAuthTokenKey); err != nil {
			return
		}

	}

	return
}

type OAuthConfig struct {
	Enabled           bool
	StaticAccessToken string
	TokenCacheName    string
	TokenCacheKey     string
}

func NewOAuth2Config() OAuthConfig {
	return OAuthConfig{
		Enabled:           false,
		StaticAccessToken: "",
		TokenCacheName:    "",
		TokenCacheKey:     "",
	}
}

type OsTokenProvider struct {
	Mgr        *service.Resources
	OAuth2Conf OAuthConfig
	Logger     *service.Logger
}

// Token returns a token or an error.
func (s OsTokenProvider) Token() (*oauth2.Token, error) {
	c := s.OAuth2Conf
	mgr := s.Mgr

	if s.OAuth2Conf.StaticAccessToken != "" {
		return &oauth2.Token{AccessToken: s.OAuth2Conf.StaticAccessToken}, nil
	}

	var tok []byte
	var terr error
	if err := mgr.AccessCache(context.Background(), c.TokenCacheName, func(cache service.Cache) {
		tok, terr = cache.Get(context.Background(), c.TokenCacheKey)
	}); err != nil {
		return nil, fmt.Errorf("failed to obtain cache resource '%v': %v", c.TokenCacheName, err)
	}
	if terr != nil {
		return nil, errors.Join(terr, fmt.Errorf("failed to obtain token wih key %v from cache", c.TokenCacheKey))
	}
	if tok == nil || string(tok) == "null" {
		return nil, errors.New("token is empty")
	}
	return &oauth2.Token{AccessToken: string(tok)}, nil

}
