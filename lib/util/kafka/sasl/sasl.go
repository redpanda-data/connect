package sasl

import (
	"context"
	"errors"
	"fmt"

	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/internal/interop"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/Shopify/sarama"
)

// SASL specific error types.
var (
	ErrUnsupportedSASLMechanism = errors.New("unsupported SASL mechanism")
)

// Config contains configuration for SASL based authentication.
// TODO: V4 Remove "enabled" and set a default mechanism
type Config struct {
	Enabled     bool   `json:"enabled" yaml:"enabled"` // DEPRECATED
	Mechanism   string `json:"mechanism" yaml:"mechanism"`
	User        string `json:"user" yaml:"user"`
	Password    string `json:"password" yaml:"password"`
	AccessToken string `json:"access_token" yaml:"access_token"`
	TokenCache  string `json:"token_cache" yaml:"token_cache"`
	TokenKey    string `json:"token_key" yaml:"token_key"`
}

// NewConfig returns a new SASL config for Kafka with default values.
func NewConfig() Config {
	return Config{} // Well, that seemed pointless.
}

// FieldSpec returns specs for SASL fields.
func FieldSpec() docs.FieldSpec {
	return docs.FieldAdvanced("sasl", "Enables SASL authentication.").WithChildren(
		docs.FieldDeprecated("enabled"),
		docs.FieldCommon("mechanism", "The SASL authentication mechanism, if left empty SASL authentication is not used. Warning: SCRAM based methods within Benthos have not received a security audit.").HasAnnotatedOptions(
			sarama.SASLTypePlaintext, "Plain text authentication. NOTE: When using plain text auth it is extremely likely that you'll also need to [enable TLS](#tlsenabled).",
			sarama.SASLTypeOAuth, "OAuth Bearer based authentication.",
			sarama.SASLTypeSCRAMSHA256, "Authentication using the SCRAM-SHA-256 mechanism.",
			sarama.SASLTypeSCRAMSHA512, "Authentication using the SCRAM-SHA-512 mechanism.",
		),
		docs.FieldCommon("user", "A `"+sarama.SASLTypePlaintext+"` username. It is recommended that you use environment variables to populate this field.", "${USER}"),
		docs.FieldCommon("password", "A `"+sarama.SASLTypePlaintext+"` password. It is recommended that you use environment variables to populate this field.", "${PASSWORD}"),
		docs.FieldAdvanced("access_token", "A static `"+sarama.SASLTypeOAuth+"` access token"),
		docs.FieldAdvanced("token_cache", "Instead of using a static `access_token` allows you to query a [`cache`](/docs/components/caches/about) resource to fetch `"+sarama.SASLTypeOAuth+"` tokens from"),
		docs.FieldAdvanced("token_key", "Required when using a `token_cache`, the key to query the cache with for tokens."),
	)
}

// Apply applies the SASL authentication configuration to a Sarama config object.
func (s Config) Apply(mgr types.Manager, conf *sarama.Config) error {
	if s.Enabled && s.Mechanism == "" {
		s.Mechanism = sarama.SASLTypePlaintext
	}
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
	case sarama.SASLTypeSCRAMSHA256:
		conf.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient {
			return &XDGSCRAMClient{HashGeneratorFcn: SHA256}
		}
		conf.Net.SASL.User = s.User
		conf.Net.SASL.Password = s.Password
	case sarama.SASLTypeSCRAMSHA512:
		conf.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient {
			return &XDGSCRAMClient{HashGeneratorFcn: SHA512}
		}
		conf.Net.SASL.User = s.User
		conf.Net.SASL.Password = s.Password
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
	mgr       types.Manager
	cacheName string
	key       string
}

func newCacheAccessTokenProvider(mgr types.Manager, cache, key string) (*cacheAccessTokenProvider, error) {
	if err := interop.ProbeCache(context.Background(), mgr, cache); err != nil {
		return nil, err
	}
	return &cacheAccessTokenProvider{
		mgr:       mgr,
		cacheName: cache,
		key:       key,
	}, nil
}

func (c *cacheAccessTokenProvider) Token() (*sarama.AccessToken, error) {
	var tok []byte
	var terr error
	if err := interop.AccessCache(context.Background(), c.mgr, c.cacheName, func(cache types.Cache) {
		tok, terr = cache.Get(c.key)
	}); err != nil {
		return nil, fmt.Errorf("failed to obtain cache resource '%v': %v", c.cacheName, err)
	}
	if terr != nil {
		return nil, terr
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
