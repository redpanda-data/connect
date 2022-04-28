package kafka

import (
	"context"
	"errors"
	"fmt"

	"github.com/Shopify/sarama"

	"github.com/benthosdev/benthos/v4/internal/component/cache"
	ksasl "github.com/benthosdev/benthos/v4/internal/impl/kafka/sasl"
	"github.com/benthosdev/benthos/v4/internal/interop"
	"github.com/benthosdev/benthos/v4/public/service"

	"github.com/twmb/franz-go/pkg/sasl"
	"github.com/twmb/franz-go/pkg/sasl/oauth"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"
)

var saslField = service.NewObjectListField("sasl",
	service.NewStringAnnotatedEnumField("mechanism", map[string]string{
		"PLAIN":         "Plain text authentication.",
		"OAUTHBEARER":   "OAuth Bearer based authentication.",
		"SCRAM-SHA-256": "SCRAM based authentication as specified in RFC5802.",
		"SCRAM-SHA-512": "SCRAM based authentication as specified in RFC5802.",
	}).
		Description("The SASL mechanism to use."),
	service.NewStringField("username").
		Description("A username to provide for PLAIN or SCRAM-* authentication.").
		Default(""),
	service.NewStringField("password").
		Description("A password to provide for PLAIN or SCRAM-* authentication.").
		Default(""),
	service.NewStringField("token").
		Description("The token to use for a single session's OAUTHBEARER authentication.").
		Default(""),
	service.NewStringMapField("extensions").
		Description("Key/value pairs to add to OAUTHBEARER authentication requests.").
		Optional(),
).
	Description("Specify one or more methods of SASL authentication. SASL is tried in order; if the broker supports the first mechanism, all connections will use that mechanism. If the first mechanism fails, the client will pick the first supported mechanism. If the broker does not support any client mechanisms, connections will fail.").
	Advanced().Optional().
	Example(
		[]interface{}{
			map[string]interface{}{
				"mechanism": "SCRAM-SHA-512",
				"username":  "foo",
				"password":  "bar",
			},
		},
	)

func saslMechanismsFromConfig(c *service.ParsedConfig) ([]sasl.Mechanism, error) {
	if !c.Contains("sasl") {
		return nil, nil
	}

	sList, err := c.FieldObjectList("sasl")
	if err != nil {
		return nil, err
	}

	mechanisms := make([]sasl.Mechanism, len(sList))
	for i, mConf := range sList {
		mechStr, err := mConf.FieldString("mechanism")
		if err == nil {
			switch mechStr {
			case "PLAIN":
				mechanisms[i], err = plainSaslFromConfig(mConf)
			case "OAUTHBEARER":
				mechanisms[i], err = oauthSaslFromConfig(mConf)
			case "SCRAM-SHA-256":
				mechanisms[i], err = scram256SaslFromConfig(mConf)
			case "SCRAM-SHA-512":
				mechanisms[i], err = scram512SaslFromConfig(mConf)
			default:
				err = fmt.Errorf("unknown mechanism: %v", mechStr)
			}
		}
		if err != nil {
			if len(sList) == 1 {
				return nil, err
			}
			return nil, fmt.Errorf("mechanism %v: %w", i, err)
		}
	}

	return mechanisms, nil
}

func plainSaslFromConfig(c *service.ParsedConfig) (sasl.Mechanism, error) {
	username, err := c.FieldString("username")
	if err != nil {
		return nil, err
	}
	password, err := c.FieldString("password")
	if err != nil {
		return nil, err
	}
	return plain.Plain(func(c context.Context) (plain.Auth, error) {
		return plain.Auth{
			User: username,
			Pass: password,
		}, nil
	}), nil
}

func oauthSaslFromConfig(c *service.ParsedConfig) (sasl.Mechanism, error) {
	token, err := c.FieldString("token")
	if err != nil {
		return nil, err
	}
	var extensions map[string]string
	if c.Contains("extensions") {
		if extensions, err = c.FieldStringMap("extensions"); err != nil {
			return nil, err
		}
	}
	return oauth.Oauth(func(c context.Context) (oauth.Auth, error) {
		return oauth.Auth{
			Token:      token,
			Extensions: extensions,
		}, nil
	}), nil
}

func scram256SaslFromConfig(c *service.ParsedConfig) (sasl.Mechanism, error) {
	username, err := c.FieldString("username")
	if err != nil {
		return nil, err
	}
	password, err := c.FieldString("password")
	if err != nil {
		return nil, err
	}
	return scram.Sha256(func(c context.Context) (scram.Auth, error) {
		return scram.Auth{
			User: username,
			Pass: password,
		}, nil
	}), nil
}

func scram512SaslFromConfig(c *service.ParsedConfig) (sasl.Mechanism, error) {
	username, err := c.FieldString("username")
	if err != nil {
		return nil, err
	}
	password, err := c.FieldString("password")
	if err != nil {
		return nil, err
	}
	return scram.Sha512(func(c context.Context) (scram.Auth, error) {
		return scram.Auth{
			User: username,
			Pass: password,
		}, nil
	}), nil
}

//------------------------------------------------------------------------------

// SASL specific error types.
var (
	ErrUnsupportedSASLMechanism = errors.New("unsupported SASL mechanism")
)

// ApplySASLConfig applies a SASL config to a sarama config.
func ApplySASLConfig(s ksasl.Config, mgr interop.Manager, conf *sarama.Config) error {
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
	case "", "none":
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
	mgr       interop.Manager
	cacheName string
	key       string
}

func newCacheAccessTokenProvider(mgr interop.Manager, cache, key string) (*cacheAccessTokenProvider, error) {
	if !mgr.ProbeCache(cache) {
		return nil, fmt.Errorf("cache resource '%v' was not found", cache)
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
	if err := c.mgr.AccessCache(context.Background(), c.cacheName, func(cache cache.V1) {
		tok, terr = cache.Get(context.Background(), c.key)
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
