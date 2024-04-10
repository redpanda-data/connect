package opensearch

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/benthosdev/benthos/v4/public/service"
	"golang.org/x/oauth2"
)

const (
	aFieldOAuth = "oauth2"

	abFieldEnabled = "enabled"

	aFieldAuthStaticAccessToken = "access_token"
	aFieldAuthTokenCache        = "token_cache"
	aFieldAuthTokenKey          = "token_key"
	aFieldAuthTokenEndpoint     = "tokenEndpoint"
	aFieldAuthTokenClientId     = "clientId"
	aFieldAuthTokenClientSecret = "clientSecret"
	aFieldAuthTokenScope        = "scope"
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
		service.NewStringField(aFieldAuthTokenEndpoint).
			Description("The endpoint to use for OAUTHBEARER token acquisition.").
			Default(""),
		service.NewStringField(aFieldAuthTokenClientId).
			Description("The client ID to use for OAUTHBEARER token acquisition.").
			Default(""),
		service.NewStringField(aFieldAuthTokenClientSecret).
			Description("The client secret to use for OAUTHBEARER token acquisition.").
			Default("").Secret(),
		service.NewStringField(aFieldAuthTokenScope).
			Description("The scope to use for OAUTHBEARER token acquisition.").
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

	res.TokenCacheKey, _ = conf.FieldString(aFieldAuthTokenKey)
	res.TokenCacheName, _ = conf.FieldString(aFieldAuthTokenCache)
	res.StaticAccessToken, _ = conf.FieldString(aFieldAuthStaticAccessToken)
	endpoint, err := conf.FieldString(aFieldAuthTokenEndpoint)
	if err == nil && endpoint != "" {
		if res.TokenEndpoint, err = conf.FieldString(aFieldAuthTokenEndpoint); err != nil {
			return
		}
		if res.ClientId, err = conf.FieldString(aFieldAuthTokenClientId); err != nil {
			return
		}
		if res.ClientSecret, err = conf.FieldString(aFieldAuthTokenClientSecret); err != nil {
			return
		}
		if res.Scope, err = conf.FieldString(aFieldAuthTokenScope); err != nil {
			return
		}
	}
	return res, nil
}

type OAuthConfig struct {
	Enabled           bool
	StaticAccessToken string
	TokenCacheName    string
	TokenCacheKey     string
	TokenEndpoint     string
	ClientId          string
	ClientSecret      string
	Scope             string
}

func (c *OAuthConfig) GetCachedToken(mgr *service.Resources) (*oauth2.Token, error) {

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

	return &oauth2.Token{
		AccessToken: string(tok),
	}, nil
}

func (c *OAuthConfig) GetToken(ctx context.Context) (*oauth2.Token, error) {

	authHeaderValue := base64.StdEncoding.EncodeToString([]byte(c.ClientId + ":" + c.ClientSecret))

	queryParams := url.Values{}
	queryParams.Set("grant_type", "client_credentials")
	queryParams.Set("scope", c.Scope)

	req, err := http.NewRequestWithContext(ctx, "POST", c.TokenEndpoint, strings.NewReader(queryParams.Encode()))
	if err != nil {
		return nil, err
	}

	req.URL.RawQuery = queryParams.Encode()

	req.Header.Set("Authorization", "Basic "+authHeaderValue)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}

	// Decode the bodyBytes into the response object
	decoder := json.NewDecoder(resp.Body)

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("token request failed with status code %d", resp.StatusCode)
	}

	var token *tokenResponseBody
	err = decoder.Decode(&token)
	if err != nil {
		panic("Unable to unmarshal response: " + err.Error())
	}

	if err := resp.Body.Close(); err != nil {
		return nil, err
	}

	duration := time.Duration(token.ExpiresIn) * time.Second

	return &oauth2.Token{
		AccessToken: token.AccessToken,
		TokenType:   token.TokenType,
		Expiry:      time.Now().Add(duration),
	}, nil

}

func NewOAuth2Config() OAuthConfig {
	return OAuthConfig{
		Enabled:           false,
		StaticAccessToken: "",
		TokenCacheName:    "",
		TokenCacheKey:     "",
		TokenEndpoint:     "",
		ClientId:          "",
		ClientSecret:      "",
		Scope:             "",
	}
}

// OsTokenProvider is an implementation of oauth2.TokenSource.
type OsTokenProvider struct {
	Mgr        *service.Resources
	OAuth2Conf OAuthConfig
	Logger     *service.Logger
}

// Token returns a token or an error.
func (s OsTokenProvider) Token() (*oauth2.Token, error) {
	mgr := s.Mgr

	if s.OAuth2Conf.StaticAccessToken != "" {
		mgr.Logger().Debug("Using static OAuth2 token for OpenSearch")
		return &oauth2.Token{AccessToken: s.OAuth2Conf.StaticAccessToken}, nil
	}

	if s.OAuth2Conf.TokenEndpoint != "" {
		return s.OAuth2Conf.GetToken(context.Background())
	}

	if s.OAuth2Conf.TokenCacheKey != "" && s.OAuth2Conf.TokenCacheName != "" {
		return s.OAuth2Conf.GetCachedToken(mgr)
	}

	return nil, errors.New("no token source available")
}

type tokenResponseBody struct {
	// AccessToken is the token that authorizes and authenticates
	// the requests.
	AccessToken string `json:"access_token"`

	// TokenType is the type of token.
	// The Type method returns either this or "Bearer", the default.
	TokenType string `json:"token_type,omitempty"`

	// RefreshToken is a token that's used by the application
	// (as opposed to the user) to refresh the access token
	// if it expires.
	RefreshToken string `json:"refresh_token,omitempty"`

	// ExpiresIn is the optional expiration time of the access token.
	//
	// If zero, TokenSource implementations will reuse the same
	// token forever and RefreshToken or equivalent
	// mechanisms for that TokenSource will not be used.
	ExpiresIn int `json:"expires_in,omitempty"`
}
