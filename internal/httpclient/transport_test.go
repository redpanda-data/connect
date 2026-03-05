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
	"context"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// --- Base transport ---

func defaultTestConfig() Config {
	return Config{
		Transport: DefaultTransportConfig(),
	}
}

func TestNewBaseTransportDefaults(t *testing.T) {
	t.Log("Given: a default config")
	cfg := defaultTestConfig()

	t.Log("When: creating a base transport")
	rt, err := newBaseTransport(cfg)
	require.NoError(t, err)
	require.NotNil(t, rt)

	t.Log("Then: the transport has HTTP/2, no TLS, and default values applied")
	tr, ok := rt.(*http.Transport)
	require.True(t, ok)
	assert.True(t, tr.ForceAttemptHTTP2)
	assert.Nil(t, tr.TLSClientConfig)
	assert.NotNil(t, tr.HTTP2)
	assert.Equal(t, 100, tr.MaxIdleConns)
	assert.Equal(t, 90*time.Second, tr.IdleConnTimeout)
	assert.Equal(t, 10*time.Second, tr.TLSHandshakeTimeout)
	assert.Equal(t, 1*time.Second, tr.ExpectContinueTimeout)
	assert.Greater(t, tr.MaxIdleConnsPerHost, 0)
}

func TestNewBaseTransportDisableHTTP2(t *testing.T) {
	t.Log("Given: a config with HTTP/2 disabled")
	cfg := defaultTestConfig()
	cfg.DisableHTTP2 = true

	t.Log("When: creating a base transport")
	rt, err := newBaseTransport(cfg)
	require.NoError(t, err)

	t.Log("Then: HTTP/2 is disabled and TLSNextProto is set")
	tr, ok := rt.(*http.Transport)
	require.True(t, ok)
	assert.False(t, tr.ForceAttemptHTTP2)
	assert.NotNil(t, tr.TLSNextProto)
	assert.Nil(t, tr.HTTP2)
}

func TestNewBaseTransportProxyURL(t *testing.T) {
	t.Log("Given: a config with a proxy URL")
	cfg := defaultTestConfig()
	cfg.ProxyURL = "http://proxy.example.com:8080"

	t.Log("When: creating a base transport")
	rt, err := newBaseTransport(cfg)
	require.NoError(t, err)

	t.Log("Then: the transport has a proxy function set")
	tr, ok := rt.(*http.Transport)
	require.True(t, ok)
	assert.NotNil(t, tr.Proxy)
}

func TestNewBaseTransportTransportConfig(t *testing.T) {
	t.Log("Given: a config with custom transport values")
	cfg := defaultTestConfig()
	cfg.Transport.MaxIdleConns = 50
	cfg.Transport.MaxIdleConnsPerHost = 10
	cfg.Transport.MaxConnsPerHost = 20
	cfg.Transport.IdleConnTimeout = 30 * time.Second
	cfg.Transport.TLSHandshakeTimeout = 5 * time.Second
	cfg.Transport.ExpectContinueTimeout = 2 * time.Second
	cfg.Transport.ResponseHeaderTimeout = 15 * time.Second
	cfg.Transport.DisableKeepAlives = true
	cfg.Transport.DisableCompression = true
	cfg.Transport.MaxResponseHeaderBytes = 1 << 20
	cfg.Transport.WriteBufferSize = 8192
	cfg.Transport.ReadBufferSize = 8192

	t.Log("When: creating a base transport")
	rt, err := newBaseTransport(cfg)
	require.NoError(t, err)

	t.Log("Then: all transport values are applied to the http.Transport")
	tr, ok := rt.(*http.Transport)
	require.True(t, ok)
	assert.Equal(t, 50, tr.MaxIdleConns)
	assert.Equal(t, 10, tr.MaxIdleConnsPerHost)
	assert.Equal(t, 20, tr.MaxConnsPerHost)
	assert.Equal(t, 30*time.Second, tr.IdleConnTimeout)
	assert.Equal(t, 5*time.Second, tr.TLSHandshakeTimeout)
	assert.Equal(t, 2*time.Second, tr.ExpectContinueTimeout)
	assert.Equal(t, 15*time.Second, tr.ResponseHeaderTimeout)
	assert.True(t, tr.DisableKeepAlives)
	assert.True(t, tr.DisableCompression)
	assert.Equal(t, int64(1<<20), tr.MaxResponseHeaderBytes)
	assert.Equal(t, 8192, tr.WriteBufferSize)
	assert.Equal(t, 8192, tr.ReadBufferSize)
}

func TestNewBaseTransportH2Config(t *testing.T) {
	t.Log("Given: a config with custom H2 transport values")
	cfg := defaultTestConfig()
	cfg.Transport.H2 = H2TransportConfig{
		StrictMaxConcurrentRequests:   true,
		MaxDecoderHeaderTableSize:     8192,
		MaxReadFrameSize:              32768,
		MaxReceiveBufferPerConnection: 2 << 20,
		SendPingTimeout:               10 * time.Second,
		PingTimeout:                   5 * time.Second,
		WriteByteTimeout:              3 * time.Second,
	}

	t.Log("When: creating a base transport")
	rt, err := newBaseTransport(cfg)
	require.NoError(t, err)

	t.Log("Then: all H2 values are applied")
	tr, ok := rt.(*http.Transport)
	require.True(t, ok)
	require.NotNil(t, tr.HTTP2)
	assert.True(t, tr.HTTP2.StrictMaxConcurrentRequests)
	assert.Equal(t, 8192, tr.HTTP2.MaxDecoderHeaderTableSize)
	assert.Equal(t, 32768, tr.HTTP2.MaxReadFrameSize)
	assert.Equal(t, 2<<20, tr.HTTP2.MaxReceiveBufferPerConnection)
	assert.Equal(t, 10*time.Second, tr.HTTP2.SendPingTimeout)
	assert.Equal(t, 5*time.Second, tr.HTTP2.PingTimeout)
	assert.Equal(t, 3*time.Second, tr.HTTP2.WriteByteTimeout)
}

// --- Auth transport ---

func TestAuthTransportBasicAuth(t *testing.T) {
	t.Log("Given: a server that captures the Authorization header")
	var gotAuth string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	t.Log("When: sending a request through an auth transport with BasicAuthSigner")
	rt := newAuthTransport(http.DefaultTransport, Config{AuthSigner: BasicAuthSigner("user", "pass")}, nil)
	req, err := http.NewRequest(http.MethodGet, srv.URL, nil)
	require.NoError(t, err)
	resp, err := rt.RoundTrip(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	t.Log("Then: the Authorization header contains basic auth credentials")
	assert.Contains(t, gotAuth, "Basic ")
}

func TestAuthTransportBearerToken(t *testing.T) {
	t.Log("Given: a server that captures the Authorization header")
	var gotAuth string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	t.Log("When: sending a request through an auth transport with BearerTokenSigner")
	rt := newAuthTransport(http.DefaultTransport, Config{AuthSigner: BearerTokenSigner("test-token-123")}, nil)
	req, err := http.NewRequest(http.MethodGet, srv.URL, nil)
	require.NoError(t, err)
	resp, err := rt.RoundTrip(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	t.Log("Then: the Authorization header contains the bearer token")
	assert.Equal(t, "Bearer test-token-123", gotAuth)
}

func TestAuthTransportSigner(t *testing.T) {
	t.Log("Given: a server that captures a custom auth header")
	var gotCustom string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotCustom = r.Header.Get("X-Custom-Auth")
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	t.Log("When: sending a request through an auth transport with a custom signer")
	signer := func(_ fs.FS, req *http.Request) error {
		req.Header.Set("X-Custom-Auth", "signed")
		return nil
	}
	rt := newAuthTransport(http.DefaultTransport, Config{AuthSigner: signer}, nil)
	req, err := http.NewRequest(http.MethodGet, srv.URL, nil)
	require.NoError(t, err)
	resp, err := rt.RoundTrip(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	t.Log("Then: the custom auth header is set by the signer")
	assert.Equal(t, "signed", gotCustom)
}

func TestAuthTransportNoAuth(t *testing.T) {
	inner := http.DefaultTransport
	rt := newAuthTransport(inner, Config{}, nil)
	assert.Equal(t, inner, rt)
}

// --- TPS transport ---

func TestTPSTransportRateLimiting(t *testing.T) {
	t.Log("Given: a server and a TPS transport at 5 RPS with burst 1")
	var count atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		count.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()
	rt := newTPSTransport(http.DefaultTransport, 5, 1)

	t.Log("When: sending 5 requests")
	start := time.Now()
	for range 5 {
		req, err := http.NewRequest(http.MethodGet, srv.URL, nil)
		require.NoError(t, err)
		resp, err := rt.RoundTrip(req)
		require.NoError(t, err)
		resp.Body.Close()
	}
	elapsed := time.Since(start)

	t.Log("Then: all 5 requests complete and rate limiting adds delay")
	assert.Equal(t, int32(5), count.Load())
	assert.GreaterOrEqual(t, elapsed, 600*time.Millisecond)
}

func TestTPSTransportBurstAllowsInitialBurst(t *testing.T) {
	t.Log("Given: a server and a TPS transport at 1 RPS with burst 5")
	var count atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		count.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()
	rt := newTPSTransport(http.DefaultTransport, 1, 5)

	t.Log("When: sending 5 requests")
	start := time.Now()
	for range 5 {
		req, err := http.NewRequest(http.MethodGet, srv.URL, nil)
		require.NoError(t, err)
		resp, err := rt.RoundTrip(req)
		require.NoError(t, err)
		resp.Body.Close()
	}
	elapsed := time.Since(start)

	t.Log("Then: all 5 complete quickly due to burst allowance")
	assert.Equal(t, int32(5), count.Load())
	assert.Less(t, elapsed, 500*time.Millisecond)
}

func TestTPSTransportDisabled(t *testing.T) {
	inner := http.DefaultTransport
	rt := newTPSTransport(inner, 0, 1)
	assert.Equal(t, inner, rt)
}

func TestTPSTransportContextCancellation(t *testing.T) {
	t.Log("Given: a very low rate TPS transport with burst 1 and a consumed token")
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	rt := newTPSTransport(http.DefaultTransport, 0.001, 1)

	// Consume the burst token.
	req, err := http.NewRequest(http.MethodGet, srv.URL, nil)
	require.NoError(t, err)
	resp, err := rt.RoundTrip(req)
	require.NoError(t, err)
	resp.Body.Close()

	t.Log("When: sending a second request with an already-cancelled context")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	req2, err := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL, nil)
	require.NoError(t, err)
	_, err = rt.RoundTrip(req2)

	t.Log("Then: the request fails with context.Canceled")
	assert.Error(t, err)
	assert.ErrorIs(t, err, context.Canceled)
}

// --- Auth transport: signer error ---

func TestAuthTransportSignerError(t *testing.T) {
	t.Log("Given: an auth transport with a signer that always fails")
	signer := func(_ fs.FS, _ *http.Request) error {
		return fmt.Errorf("signing failed")
	}
	rt := newAuthTransport(http.DefaultTransport, Config{AuthSigner: signer}, nil)

	t.Log("When: sending a request")
	req, err := http.NewRequest(http.MethodGet, "http://localhost", nil)
	require.NoError(t, err)
	_, err = rt.RoundTrip(req)

	t.Log("Then: the signer error is propagated")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "signing failed")
}

// --- Max body transport ---

func TestMaxBodyTransportTruncatesBody(t *testing.T) {
	t.Log("Given: a server returning 10 bytes and a max body transport limited to 5")
	body := "abcdefghij"
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		fmt.Fprint(w, body)
	}))
	defer srv.Close()
	rt := newMaxBodyTransport(http.DefaultTransport, 5)

	t.Log("When: reading the response body")
	req, err := http.NewRequest(http.MethodGet, srv.URL, nil)
	require.NoError(t, err)
	resp, err := rt.RoundTrip(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	t.Log("Then: the body is truncated to 5 bytes")
	assert.Equal(t, "abcde", string(data))
}

func TestMaxBodyTransportNilBody(t *testing.T) {
	t.Log("Given: a server returning 204 with no body")
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()
	rt := newMaxBodyTransport(http.DefaultTransport, 100)

	t.Log("When: sending a request")
	req, err := http.NewRequest(http.MethodGet, srv.URL, nil)
	require.NoError(t, err)
	resp, err := rt.RoundTrip(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	t.Log("Then: the response is 204 with no error")
	assert.Equal(t, http.StatusNoContent, resp.StatusCode)
}

func TestMaxBodyTransportDisabled(t *testing.T) {
	inner := http.DefaultTransport
	rt := newMaxBodyTransport(inner, 0)
	assert.Equal(t, inner, rt)

	rt = newMaxBodyTransport(inner, -1)
	assert.Equal(t, inner, rt)
}
