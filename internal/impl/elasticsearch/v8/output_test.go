// Copyright 2025 Redpanda Data, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package elasticsearch

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"
)

func parseOutputConfig(t *testing.T, yaml string) *esOutput {
	t.Helper()

	conf, err := elasticsearchConfigSpec().ParseYAML(yaml, service.NewEnvironment())
	require.NoError(t, err)

	out, err := outputFromParsed(conf, service.MockResources())
	require.NoError(t, err)
	return out
}

// A zero value http.Transport has no dial, TLS handshake or idle connection
// timeout, so building one from scratch silently discards the protections that
// elastictransport applies when it is left to clone http.DefaultTransport.
func TestConfigTLSRetainsDefaultTransportTimeouts(t *testing.T) {
	out := parseOutputConfig(t, `
urls: [ https://localhost:9200 ]
index: testing
action: index
id: ""
tls:
  enabled: true
`)

	transport, ok := out.conf.clientOpts.Transport.(*http.Transport)
	require.True(t, ok)

	defaults, ok := http.DefaultTransport.(*http.Transport)
	require.True(t, ok)

	assert.NotNil(t, transport.TLSClientConfig, "the configured TLS settings should be applied")
	assert.NotNil(t, transport.DialContext, "a nil DialContext dials without any timeout")
	assert.Equal(t, defaults.TLSHandshakeTimeout, transport.TLSHandshakeTimeout)
	assert.Equal(t, defaults.IdleConnTimeout, transport.IdleConnTimeout)
	assert.Equal(t, defaults.ExpectContinueTimeout, transport.ExpectContinueTimeout)
}
