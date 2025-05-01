// Copyright 2024 Redpanda Data, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package kafka_test

import (
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
	require.NoError(t, mockResources.AccessCache(t.Context(), "token_provider", func(c service.Cache) {
		require.NoError(t, c.Add(t.Context(), "jwt", []byte("foo"), nil))
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
