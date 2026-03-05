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
	"crypto/tls"
	"fmt"
	"io"
	"io/fs"
	"net"
	"net/http"
	"net/url"

	"golang.org/x/time/rate"

	"github.com/redpanda-data/benthos/v4/public/utils/netutil"
)

// --- Base transport ---

// newBaseTransport creates the innermost http.RoundTripper with TLS, proxy,
// and HTTP/2 settings applied.
func newBaseTransport(cfg Config) (http.RoundTripper, error) {
	d := new(net.Dialer)
	if err := netutil.DecorateDialer(d, cfg.Dialer); err != nil {
		return nil, err
	}

	tc := cfg.Transport
	tr := &http.Transport{
		Proxy:                  http.ProxyFromEnvironment,
		DialContext:            d.DialContext,
		MaxIdleConns:           tc.MaxIdleConns,
		MaxIdleConnsPerHost:    tc.MaxIdleConnsPerHost,
		MaxConnsPerHost:        tc.MaxConnsPerHost,
		IdleConnTimeout:        tc.IdleConnTimeout,
		TLSHandshakeTimeout:    tc.TLSHandshakeTimeout,
		ExpectContinueTimeout:  tc.ExpectContinueTimeout,
		ResponseHeaderTimeout:  tc.ResponseHeaderTimeout,
		DisableKeepAlives:      tc.DisableKeepAlives,
		DisableCompression:     tc.DisableCompression,
		MaxResponseHeaderBytes: tc.MaxResponseHeaderBytes,
		WriteBufferSize:        tc.WriteBufferSize,
		ReadBufferSize:         tc.ReadBufferSize,
		ForceAttemptHTTP2:      !cfg.DisableHTTP2,
	}
	if cfg.ProxyURL != "" {
		p, err := url.Parse(cfg.ProxyURL)
		if err != nil {
			return nil, fmt.Errorf("invalid proxy_url %q: %w", cfg.ProxyURL, err)
		}
		tr.Proxy = http.ProxyURL(p)
	}

	if !cfg.DisableHTTP2 {
		h2 := tc.H2
		tr.HTTP2 = &http.HTTP2Config{
			StrictMaxConcurrentRequests:   h2.StrictMaxConcurrentRequests,
			MaxDecoderHeaderTableSize:     h2.MaxDecoderHeaderTableSize,
			MaxEncoderHeaderTableSize:     h2.MaxEncoderHeaderTableSize,
			MaxReadFrameSize:              h2.MaxReadFrameSize,
			MaxReceiveBufferPerConnection: h2.MaxReceiveBufferPerConnection,
			MaxReceiveBufferPerStream:     h2.MaxReceiveBufferPerStream,
			SendPingTimeout:               h2.SendPingTimeout,
			PingTimeout:                   h2.PingTimeout,
			WriteByteTimeout:              h2.WriteByteTimeout,
		}
	} else {
		// Setting TLSNextProto to a non-nil empty map disables HTTP/2.
		tr.TLSNextProto = map[string]func(string, *tls.Conn) http.RoundTripper{}
	}

	if cfg.TLSEnabled && cfg.TLSConf != nil {
		tr.TLSClientConfig = cfg.TLSConf
	}

	return tr, nil
}

// --- Auth transport ---

// authTransport applies authentication to outgoing requests via the
// product-supplied AuthSigner function.
type authTransport struct {
	inner    http.RoundTripper
	signer   func(fs.FS, *http.Request) error
	signerFS fs.FS
}

var _ http.RoundTripper = (*authTransport)(nil)

func newAuthTransport(inner http.RoundTripper, cfg Config, signerFS fs.FS) http.RoundTripper {
	if cfg.AuthSigner == nil {
		return inner
	}
	return &authTransport{
		inner:    inner,
		signer:   cfg.AuthSigner,
		signerFS: signerFS,
	}
}

func (t *authTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if err := t.signer(t.signerFS, req); err != nil {
		return nil, err
	}
	return t.inner.RoundTrip(req)
}

// --- TPS transport ---

// tpsTransport rate-limits outgoing requests with a token bucket.
type tpsTransport struct {
	inner   http.RoundTripper
	limiter *rate.Limiter
}

var _ http.RoundTripper = (*tpsTransport)(nil)

func newTPSTransport(inner http.RoundTripper, tpsLimit float64, tpsBurst int) http.RoundTripper {
	if tpsLimit <= 0 {
		return inner
	}
	if tpsBurst < 1 {
		tpsBurst = 1
	}
	return &tpsTransport{
		inner:   inner,
		limiter: rate.NewLimiter(rate.Limit(tpsLimit), tpsBurst),
	}
}

func (t *tpsTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if err := t.limiter.Wait(req.Context()); err != nil {
		return nil, err
	}
	return t.inner.RoundTrip(req)
}

// readCloser combines a Reader and Closer into an io.ReadCloser.
type readCloser struct {
	io.Reader
	io.Closer
}

// --- Max response body transport ---

// maxBodyTransport caps response bodies with an io.LimitReader.
type maxBodyTransport struct {
	inner    http.RoundTripper
	maxBytes int64
}

var _ http.RoundTripper = (*maxBodyTransport)(nil)

func newMaxBodyTransport(inner http.RoundTripper, maxBytes int64) http.RoundTripper {
	if maxBytes <= 0 {
		return inner
	}
	return &maxBodyTransport{inner: inner, maxBytes: maxBytes}
}

func (t *maxBodyTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	resp, err := t.inner.RoundTrip(req)
	if err != nil {
		return resp, err
	}
	if resp.Body != nil {
		resp.Body = readCloser{
			Reader: io.LimitReader(resp.Body, t.maxBytes),
			Closer: resp.Body,
		}
	}
	return resp, nil
}
