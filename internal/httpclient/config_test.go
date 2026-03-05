// Copyright 2026 Redpanda Data, Inc.
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

package httpclient

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"
)

// configSpec builds a ConfigSpec from Fields() for use in tests.
func configSpec() *service.ConfigSpec {
	spec := service.NewConfigSpec()
	for _, f := range Fields() {
		spec = spec.Field(f)
	}
	return spec
}

func parseTestYAML(t *testing.T, yaml string) *service.ParsedConfig {
	t.Helper()
	env := service.NewEnvironment()
	pConf, err := configSpec().ParseYAML(yaml, env)
	require.NoError(t, err)
	return pConf
}

func TestNewConfigFromParsedDefaults(t *testing.T) {
	t.Log("Given: a YAML config with only base_url (all other fields use defaults)")
	pConf := parseTestYAML(t, `base_url: "https://example.com"`)

	t.Log("When: parsing the config")
	cfg, err := NewConfigFromParsed(pConf)
	require.NoError(t, err)

	t.Log("Then: base_url is parsed and TLS auto-enabled for https")
	assert.Equal(t, "https://example.com", cfg.BaseURL)
	assert.True(t, cfg.TLSEnabled)
	assert.NotNil(t, cfg.TLSConf)
	assert.Empty(t, cfg.ProxyURL)
	assert.False(t, cfg.DisableHTTP2)
	assert.Nil(t, cfg.AuthSigner)
	assert.Equal(t, 0.0, cfg.TPSLimit)
	assert.Equal(t, 1, cfg.TPSBurst)
	assert.Equal(t, 1*time.Second, cfg.BackoffInitialInterval)
	assert.Equal(t, 30*time.Second, cfg.BackoffMaxInterval)
	assert.Equal(t, 3, cfg.BackoffMaxRetries)
	assert.Empty(t, cfg.AccessLogLevel)
	assert.Equal(t, 0, cfg.AccessLogBodyLimit)

	t.Log("Then: transport fields have expected defaults")
	tc := cfg.Transport
	assert.Equal(t, 100, tc.MaxIdleConns)
	assert.Greater(t, tc.MaxIdleConnsPerHost, 0)
	assert.Equal(t, 64, tc.MaxConnsPerHost)
	assert.Equal(t, 90*time.Second, tc.IdleConnTimeout)
	assert.Equal(t, 10*time.Second, tc.TLSHandshakeTimeout)
	assert.Equal(t, 1*time.Second, tc.ExpectContinueTimeout)
	assert.Equal(t, time.Duration(0), tc.ResponseHeaderTimeout)
	assert.False(t, tc.DisableKeepAlives)
	assert.False(t, tc.DisableCompression)
	assert.Equal(t, int64(1<<20), tc.MaxResponseHeaderBytes)
	assert.Equal(t, int64(10<<20), tc.MaxResponseBodyBytes)
	assert.Equal(t, 4096, tc.WriteBufferSize)
	assert.Equal(t, 4096, tc.ReadBufferSize)

	t.Log("Then: H2 fields have expected defaults")
	h2 := tc.H2
	assert.False(t, h2.StrictMaxConcurrentRequests)
	assert.Equal(t, 4096, h2.MaxDecoderHeaderTableSize)
	assert.Equal(t, 4096, h2.MaxEncoderHeaderTableSize)
	assert.Equal(t, 16384, h2.MaxReadFrameSize)
	assert.Equal(t, 1<<20, h2.MaxReceiveBufferPerConnection)
	assert.Equal(t, 1<<20, h2.MaxReceiveBufferPerStream)
	assert.Equal(t, time.Duration(0), h2.SendPingTimeout)
	assert.Equal(t, 15*time.Second, h2.PingTimeout)
	assert.Equal(t, time.Duration(0), h2.WriteByteTimeout)
}

func TestNewConfigFromParsedAllFieldsSet(t *testing.T) {
	t.Log("Given: a YAML config with every field explicitly set")
	yaml := `
base_url: "http://api.example.com"
timeout: 10s
proxy_url: http://proxy.example.com:8080
disable_http2: true
tps_limit: 50.0
tps_burst: 10
backoff:
  initial_interval: 2s
  max_interval: 60s
  max_retries: 5
access_log_level: DEBUG
access_log_body_limit: 1024
http:
  max_idle_conns: 200
  max_idle_conns_per_host: 25
  max_conns_per_host: 50
  idle_conn_timeout: 120s
  tls_handshake_timeout: 5s
  expect_continue_timeout: 3s
  response_header_timeout: 20s
  disable_keep_alives: true
  disable_compression: true
  max_response_header_bytes: 2097152
  max_response_body_bytes: 52428800
  write_buffer_size: 8192
  read_buffer_size: 16384
  h2:
    strict_max_concurrent_requests: true
    max_decoder_header_table_size: 8192
    max_encoder_header_table_size: 8192
    max_read_frame_size: 32768
    max_receive_buffer_per_connection: 2097152
    max_receive_buffer_per_stream: 2097152
    send_ping_timeout: 5s
    ping_timeout: 10s
    write_byte_timeout: 3s
`

	t.Log("When: parsing the config")
	pConf := parseTestYAML(t, yaml)
	cfg, err := NewConfigFromParsed(pConf)
	require.NoError(t, err)

	t.Log("Then: all top-level fields match the YAML values")
	assert.Equal(t, "http://api.example.com", cfg.BaseURL)
	assert.Equal(t, 10*time.Second, cfg.Timeout)
	assert.Equal(t, "http://proxy.example.com:8080", cfg.ProxyURL)
	assert.True(t, cfg.DisableHTTP2)
	assert.Equal(t, 50.0, cfg.TPSLimit)
	assert.Equal(t, 10, cfg.TPSBurst)
	assert.Equal(t, 2*time.Second, cfg.BackoffInitialInterval)
	assert.Equal(t, 60*time.Second, cfg.BackoffMaxInterval)
	assert.Equal(t, 5, cfg.BackoffMaxRetries)
	assert.Equal(t, "DEBUG", cfg.AccessLogLevel)
	assert.Equal(t, 1024, cfg.AccessLogBodyLimit)

	t.Log("Then: transport fields match the YAML values")
	tc := cfg.Transport
	assert.Equal(t, 200, tc.MaxIdleConns)
	assert.Equal(t, 25, tc.MaxIdleConnsPerHost)
	assert.Equal(t, 50, tc.MaxConnsPerHost)
	assert.Equal(t, 120*time.Second, tc.IdleConnTimeout)
	assert.Equal(t, 5*time.Second, tc.TLSHandshakeTimeout)
	assert.Equal(t, 3*time.Second, tc.ExpectContinueTimeout)
	assert.Equal(t, 20*time.Second, tc.ResponseHeaderTimeout)
	assert.True(t, tc.DisableKeepAlives)
	assert.True(t, tc.DisableCompression)
	assert.Equal(t, int64(2097152), tc.MaxResponseHeaderBytes)
	assert.Equal(t, int64(52428800), tc.MaxResponseBodyBytes)
	assert.Equal(t, 8192, tc.WriteBufferSize)
	assert.Equal(t, 16384, tc.ReadBufferSize)

	t.Log("Then: H2 fields match the YAML values")
	h2 := tc.H2
	assert.True(t, h2.StrictMaxConcurrentRequests)
	assert.Equal(t, 8192, h2.MaxDecoderHeaderTableSize)
	assert.Equal(t, 8192, h2.MaxEncoderHeaderTableSize)
	assert.Equal(t, 32768, h2.MaxReadFrameSize)
	assert.Equal(t, 2097152, h2.MaxReceiveBufferPerConnection)
	assert.Equal(t, 2097152, h2.MaxReceiveBufferPerStream)
	assert.Equal(t, 5*time.Second, h2.SendPingTimeout)
	assert.Equal(t, 10*time.Second, h2.PingTimeout)
	assert.Equal(t, 3*time.Second, h2.WriteByteTimeout)
}

func TestValidateH2Config(t *testing.T) {
	valid := DefaultH2TransportConfig()

	t.Run("valid defaults", func(t *testing.T) {
		assert.NoError(t, validateH2Config(valid))
	})

	t.Run("decoder header table too large", func(t *testing.T) {
		h2 := valid
		h2.MaxDecoderHeaderTableSize = 4 << 20
		assert.ErrorContains(t, validateH2Config(h2), "max_decoder_header_table_size")
	})

	t.Run("encoder header table too large", func(t *testing.T) {
		h2 := valid
		h2.MaxEncoderHeaderTableSize = 4 << 20
		assert.ErrorContains(t, validateH2Config(h2), "max_encoder_header_table_size")
	})

	t.Run("read frame size too small", func(t *testing.T) {
		h2 := valid
		h2.MaxReadFrameSize = 1024
		assert.ErrorContains(t, validateH2Config(h2), "max_read_frame_size")
	})

	t.Run("read frame size too large", func(t *testing.T) {
		h2 := valid
		h2.MaxReadFrameSize = 17 << 20
		assert.ErrorContains(t, validateH2Config(h2), "max_read_frame_size")
	})

	t.Run("recv buffer per conn too small", func(t *testing.T) {
		h2 := valid
		h2.MaxReceiveBufferPerConnection = 1024
		assert.ErrorContains(t, validateH2Config(h2), "max_receive_buffer_per_connection")
	})

	t.Run("recv buffer per conn too large", func(t *testing.T) {
		h2 := valid
		h2.MaxReceiveBufferPerConnection = 4 << 20
		assert.ErrorContains(t, validateH2Config(h2), "max_receive_buffer_per_connection")
	})

	t.Run("recv buffer per stream too large", func(t *testing.T) {
		h2 := valid
		h2.MaxReceiveBufferPerStream = 4 << 20
		assert.ErrorContains(t, validateH2Config(h2), "max_receive_buffer_per_stream")
	})
}

func TestValidateH2ConfigViaYAML(t *testing.T) {
	t.Log("Given: a YAML config with an invalid H2 max_read_frame_size")
	yaml := `
base_url: "https://example.com"
http:
  h2:
    max_read_frame_size: 100
`

	t.Log("When: parsing the config through NewConfigFromParsed")
	pConf := parseTestYAML(t, yaml)
	_, err := NewConfigFromParsed(pConf)

	t.Log("Then: validation rejects the invalid H2 value")
	assert.ErrorContains(t, err, "max_read_frame_size")
}
