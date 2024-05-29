package kafka_test

import (
	"context"
	"testing"

	"github.com/IBM/sarama"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/connect/v4/internal/impl/kafka"

	_ "github.com/redpanda-data/benthos/v4/public/components/pure"
	"github.com/redpanda-data/benthos/v4/public/service"
)

func TestApplyPlaintext(t *testing.T) {
	saslConf := service.NewConfigSpec().Field(kafka.SaramaSASLField())
	pConf, err := saslConf.ParseYAML(`
sasl:
  mechanism: PLAIN
  user: foo
  password: bar
`, nil)
	require.NoError(t, err)

	conf := &sarama.Config{}
	require.NoError(t, kafka.ApplySaramaSASLFromParsed(pConf, service.MockResources(), conf))

	if !conf.Net.SASL.Enable {
		t.Errorf("SASL not enabled")
	}

	if conf.Net.SASL.Mechanism != sarama.SASLTypePlaintext {
		t.Errorf("Wrong SASL mechanism: %v != %v", conf.Net.SASL.Mechanism, sarama.SASLTypePlaintext)
	}

	if conf.Net.SASL.User != "foo" {
		t.Errorf("Wrong SASL user: %v != %v", conf.Net.SASL.User, "foo")
	}

	if conf.Net.SASL.Password != "bar" {
		t.Errorf("Wrong SASL password: %v != %v", conf.Net.SASL.Password, "bar")
	}
}

func TestApplyOAuthBearerStaticProvider(t *testing.T) {
	saslConf := service.NewConfigSpec().Field(kafka.SaramaSASLField())
	pConf, err := saslConf.ParseYAML(`
sasl:
  mechanism: OAUTHBEARER
  access_token: foo
`, nil)
	require.NoError(t, err)

	conf := &sarama.Config{}
	require.NoError(t, kafka.ApplySaramaSASLFromParsed(pConf, service.MockResources(), conf))

	if !conf.Net.SASL.Enable {
		t.Errorf("SASL not enabled")
	}

	if conf.Net.SASL.Mechanism != sarama.SASLTypeOAuth {
		t.Errorf("Wrong SASL mechanism: %v != %v", conf.Net.SASL.Mechanism, sarama.SASLTypeOAuth)
	}

	token, err := conf.Net.SASL.TokenProvider.Token()
	if err != nil {
		t.Errorf("Failed to get token")
	}

	if act := token.Token; act != "foo" {
		t.Errorf("Wrong SASL token: %v != %v", act, "foo")
	}
}

func TestApplyOAuthBearerCacheProvider(t *testing.T) {
	saslConf := service.NewConfigSpec().Field(kafka.SaramaSASLField())
	pConf, err := saslConf.ParseYAML(`
sasl:
  mechanism: OAUTHBEARER
  token_cache: token_provider
  token_key: jwt
`, nil)
	require.NoError(t, err)

	mockResources := service.MockResources(service.MockResourcesOptAddCache("token_provider"))
	require.NoError(t, mockResources.AccessCache(context.Background(), "token_provider", func(c service.Cache) {
		require.NoError(t, c.Add(context.Background(), "jwt", []byte("foo"), nil))
	}))

	conf := &sarama.Config{}
	require.NoError(t, kafka.ApplySaramaSASLFromParsed(pConf, mockResources, conf))

	if !conf.Net.SASL.Enable {
		t.Errorf("SASL not enabled")
	}

	if conf.Net.SASL.Mechanism != sarama.SASLTypeOAuth {
		t.Errorf("Wrong SASL mechanism: %v != %v", conf.Net.SASL.Mechanism, sarama.SASLTypeOAuth)
	}

	token, err := conf.Net.SASL.TokenProvider.Token()
	if err != nil {
		t.Errorf("Failed to get token")
	}

	if act := token.Token; act != "foo" {
		t.Errorf("Wrong SASL token: %v != %v", act, "foo")
	}

	// Test with missing key
	pConf, err = saslConf.ParseYAML(`
sasl:
  mechanism: OAUTHBEARER
  token_cache: token_provider
  token_key: bar
`, nil)
	require.NoError(t, err)

	conf = &sarama.Config{}
	require.NoError(t, kafka.ApplySaramaSASLFromParsed(pConf, mockResources, conf))

	if _, err := conf.Net.SASL.TokenProvider.Token(); err == nil {
		t.Errorf("Expected failure to get token")
	}
}

func TestApplyUnknownMechanism(t *testing.T) {
	saslConf := service.NewConfigSpec().Field(kafka.SaramaSASLField())
	pConf, err := saslConf.ParseYAML(`
sasl:
  mechanism: foo
`, nil)
	require.NoError(t, err)

	conf := &sarama.Config{}
	require.Error(t, kafka.ApplySaramaSASLFromParsed(pConf, service.MockResources(), conf))
}
