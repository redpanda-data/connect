package sasl

import (
	"testing"

	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/Shopify/sarama"
)

//------------------------------------------------------------------------------

type mockMgr struct {
	cache mockCache
	types.DudMgr
}

type mockCache struct {
	entries map[string]string
	types.Cache
}

func (m mockMgr) GetCache(name string) (types.Cache, error) {
	return m.cache, nil
}

func (c mockCache) Get(key string) ([]byte, error) {
	v, ok := c.entries[key]
	if !ok {
		return nil, types.ErrKeyNotFound
	}

	return []byte(v), nil
}

//------------------------------------------------------------------------------

func TestApplyPlaintext(t *testing.T) {
	conf := &sarama.Config{}

	saslConf := Config{
		Mechanism: string(sarama.SASLTypePlaintext),
		User:      "foo",
		Password:  "bar",
	}

	err := saslConf.Apply(types.NoopMgr(), conf)
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

	saslConf := Config{
		Enabled:   true,
		Mechanism: "",
		User:      "foo",
		Password:  "bar",
	}

	err := saslConf.Apply(types.NoopMgr(), conf)
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

	saslConf := Config{
		Mechanism:   string(sarama.SASLTypeOAuth),
		AccessToken: "foo",
	}

	err := saslConf.Apply(types.NoopMgr(), conf)
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

	saslConf := Config{
		Mechanism:  string(sarama.SASLTypeOAuth),
		TokenCache: "token_provider",
		TokenKey:   "jwt",
	}

	cache := mockCache{
		entries: map[string]string{"jwt": "foo"},
	}
	err := saslConf.Apply(mockMgr{cache: cache}, conf)
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

	// Test with missing key
	saslConf.TokenKey = "bar"
	err = saslConf.Apply(mockMgr{cache: cache}, conf)
	if err != nil {
		t.Fatal(err)
	}

	if _, err := conf.Net.SASL.TokenProvider.Token(); err == nil {
		t.Errorf("Expected failure to get token")
	}
}

func TestApplyUnknownMechanism(t *testing.T) {
	conf := &sarama.Config{}

	saslConf := Config{
		Mechanism: "foo",
	}

	err := saslConf.Apply(types.NoopMgr(), conf)
	if err != ErrUnsupportedSASLMechanism {
		t.Errorf("Err %v != %v", err, ErrUnsupportedSASLMechanism)
	}
}

//------------------------------------------------------------------------------
