package opensearch

import (
	"context"
	"fmt"

	"github.com/benthosdev/benthos/v4/public/service"
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
		service.NewStringField(aFieldAuthStaticAccessToken).
			Description("A static access token to use for authentication.").
			Advanced().
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

	if conf.Contains(aFieldAuthStaticAccessToken) {
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

func (c *OAuthConfig) GetToken(mgr *service.Resources) (string, error) {
	if c.StaticAccessToken != "" {
		return c.StaticAccessToken, nil
	}

	var tok []byte
	var terr error
	if err := mgr.AccessCache(context.Background(), c.TokenCacheName, func(cache service.Cache) {
		tok, terr = cache.Get(context.Background(), c.TokenCacheKey)
	}); err != nil {
		return "", fmt.Errorf("failed to obtain cache resource '%v': %v", c.TokenCacheName, err)
	}
	if terr != nil {
		return "", terr
	}
	return string(tok), nil
}

func NewOAuth2Config() OAuthConfig {
	return OAuthConfig{
		Enabled:           false,
		StaticAccessToken: "",
		TokenCacheName:    "",
		TokenCacheKey:     "",
	}
}
