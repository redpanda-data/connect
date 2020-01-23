package kafka

import (
	"errors"

	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/Shopify/sarama"
)

var (
	ErrUnsupportedSASLMechanism = errors.New("unsupported SASL mechanism")
)

// SASLConfig contains configuration for SASL based authentication.
type SASLConfig struct {
	Mechanism   string `json:"mechanism" yaml:"mechanism"`
	User        string `json:"user" yaml:"user"`
	Password    string `json:"password" yaml:"password"`
	AccessToken string `json:"access_token" yaml:"access_token"`
	TokenKey    string `json:"token_key" yaml:"token_key"`
	TokenCache  string `json:"token_cache" yaml:"token_cache"`
}

// Apply applies the SASL authentication configuration to a Sarama config object.
func (s SASLConfig) Apply(mgr types.Manager, conf *sarama.Config) error {
	switch s.Mechanism {
	case sarama.SASLTypeOAuth:
		var tp sarama.AccessTokenProvider
		var err error

		if s.TokenCache != "" {
			tp, err = newCacheAccessTokenProvider(mgr, s.TokenCache, s.TokenKey)
			if err != nil {
				return err
			}
		} else {
			tp, err = newStaticAccessTokenProvider(s.AccessToken)
			if err != nil {
				return err
			}
		}
		conf.Net.SASL.TokenProvider = tp
	case sarama.SASLTypePlaintext:
		conf.Net.SASL.User = s.User
		conf.Net.SASL.Password = s.Password
	case "":
		return nil
	default:
		return ErrUnsupportedSASLMechanism
	}

	conf.Net.SASL.Enable = true
	conf.Net.SASL.Mechanism = sarama.SASLMechanism(s.Mechanism)

	return nil
}

//------------------------------------------------------------------------------

// cacheAccessTokenProvider fetches SASL OAUTHBEARER access tokens from a cache.
type cacheAccessTokenProvider struct {
	cache types.Cache
	key   string
}

func newCacheAccessTokenProvider(mgr types.Manager, cache string, key string) (*cacheAccessTokenProvider, error) {
	c, err := mgr.GetCache(cache)
	if err != nil {
		return nil, err
	}

	return &cacheAccessTokenProvider{c, key}, nil
}

func (c *cacheAccessTokenProvider) Token() (*sarama.AccessToken, error) {
	tok, err := c.cache.Get(c.key)
	if err != nil {
		return nil, err
	}

	return &sarama.AccessToken{Token: string(tok)}, nil
}

//------------------------------------------------------------------------------

// staticAccessTokenProvider provides a static SASL OAUTHBEARER access token.
type staticAccessTokenProvider struct {
	token string
}

func newStaticAccessTokenProvider(token string) (*staticAccessTokenProvider, error) {
	return &staticAccessTokenProvider{token}, nil
}

func (s *staticAccessTokenProvider) Token() (*sarama.AccessToken, error) {
	return &sarama.AccessToken{Token: s.token}, nil
}

//------------------------------------------------------------------------------
