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
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

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

// unresponsiveServer accepts a request and then never answers it, which is the
// shape of the stall that the timeout exists to break. The handler is released
// by the test rather than by the request context: the server only notices a
// client going away once the request body has been consumed, and a handler that
// reads nothing would otherwise block Close forever.
func unresponsiveServer(t *testing.T) (url string, served *atomic.Bool) {
	t.Helper()

	served = &atomic.Bool{}
	release := make(chan struct{})

	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		served.Store(true)
		<-release
	}))

	t.Cleanup(server.Close)
	t.Cleanup(func() { close(release) })

	return server.URL, served
}

func TestConfigTimeout(t *testing.T) {
	tests := []struct {
		name  string
		field string
		want  time.Duration
	}{
		{name: "unset waits indefinitely", field: "", want: 0},
		{name: "explicit value", field: "timeout: 5s", want: 5 * time.Second},
		{name: "explicit zero waits indefinitely", field: "timeout: 0s", want: 0},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			out := parseOutputConfig(t, fmt.Sprintf(`
urls: [ http://localhost:9200 ]
index: testing
action: index
id: ""
%v
`, test.field))
			assert.Equal(t, test.want, out.conf.timeout)
		})
	}
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

func TestWriteBatchTimesOutOnUnresponsiveServer(t *testing.T) {
	const timeout = 250 * time.Millisecond

	url, served := unresponsiveServer(t)

	out := parseOutputConfig(t, fmt.Sprintf(`
urls: [ %v ]
index: testing
action: index
id: ""
timeout: %v
`, url, timeout))

	require.NoError(t, out.Connect(t.Context()))

	batch := service.MessageBatch{service.NewMessage([]byte(`{"hello":"world"}`))}

	start := time.Now()
	err := out.WriteBatch(t.Context(), batch)
	elapsed := time.Since(start)

	require.Error(t, err)
	assert.ErrorIs(t, err, context.DeadlineExceeded)
	assert.Less(t, elapsed, 10*timeout, "the write should be abandoned at roughly the configured timeout")
	assert.True(t, served.Load(), "expected the request to reach the server")
}

// With the timeout unset, which is the default, the only deadline left is the
// caller's. This is the behaviour of the output before the field existed.
func TestWriteBatchZeroTimeoutDefersToCallerContext(t *testing.T) {
	url, _ := unresponsiveServer(t)

	out := parseOutputConfig(t, fmt.Sprintf(`
urls: [ %v ]
index: testing
action: index
id: ""
timeout: 0s
`, url))

	require.NoError(t, out.Connect(t.Context()))

	ctx, cancel := context.WithTimeout(t.Context(), 250*time.Millisecond)
	defer cancel()

	batch := service.MessageBatch{service.NewMessage([]byte(`{"hello":"world"}`))}

	err := out.WriteBatch(ctx, batch)
	require.Error(t, err)
	assert.ErrorIs(t, err, context.DeadlineExceeded)
}
