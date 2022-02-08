package sasl_test

import (
	"testing"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/manager"
	"github.com/Jeffail/benthos/v3/lib/manager/mock"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/util/kafka/sasl"
	"github.com/Shopify/sarama"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"

	_ "github.com/Jeffail/benthos/v3/public/components/all"
)

func TestApplyPlaintext(t *testing.T) {
	conf := &sarama.Config{}

	saslConf := sasl.Config{
		Mechanism: string(sarama.SASLTypePlaintext),
		User:      "foo",
		Password:  "bar",
	}

	err := saslConf.Apply(mock.NewManager(), conf)
	if err != nil {
		t.Fatal(err)
	}

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

func TestApplyPlaintextDeprecated(t *testing.T) {
	conf := &sarama.Config{}

	saslConf := sasl.Config{
		Enabled:   true,
		Mechanism: "",
		User:      "foo",
		Password:  "bar",
	}

	err := saslConf.Apply(mock.NewManager(), conf)
	if err != nil {
		t.Fatal(err)
	}

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
	conf := &sarama.Config{}

	saslConf := sasl.Config{
		Mechanism:   string(sarama.SASLTypeOAuth),
		AccessToken: "foo",
	}

	err := saslConf.Apply(mock.NewManager(), conf)
	if err != nil {
		t.Fatal(err)
	}

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
	conf := &sarama.Config{}

	saslConf := sasl.Config{
		Mechanism:  string(sarama.SASLTypeOAuth),
		TokenCache: "token_provider",
		TokenKey:   "jwt",
	}

	resConf := manager.NewResourceConfig()
	require.NoError(t, yaml.Unmarshal([]byte(`
cache_resources:
  - label: token_provider
    memory:
      init_values:
        jwt: foo
`), &resConf))

	mgr, err := manager.NewV2(resConf, mock.NewManager(), log.Noop(), metrics.Noop())
	require.NoError(t, err)

	require.NoError(t, saslConf.Apply(mgr, conf))

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
	saslConf.TokenKey = "bar"
	err = saslConf.Apply(mgr, conf)
	if err != nil {
		t.Fatal(err)
	}

	if _, err := conf.Net.SASL.TokenProvider.Token(); err == nil {
		t.Errorf("Expected failure to get token")
	}
}

func TestApplyUnknownMechanism(t *testing.T) {
	conf := &sarama.Config{}

	saslConf := sasl.Config{
		Mechanism: "foo",
	}

	err := saslConf.Apply(mock.NewManager(), conf)
	if err != sasl.ErrUnsupportedSASLMechanism {
		t.Errorf("Err %v != %v", err, sasl.ErrUnsupportedSASLMechanism)
	}
}

//------------------------------------------------------------------------------
