// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package gateway_test

import (
	"crypto/hmac"
	"crypto/sha256"
	"crypto/sha512"
	"encoding/hex"
	"errors"
	"hash"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/connect/v4/internal/gateway"
	"github.com/redpanda-data/connect/v4/internal/license"
)

// defaultTestMaxBodySize is a generous max body size used by tests that are
// not exercising the body-size cap itself.
const defaultTestMaxBodySize = 4 * 1024 * 1024

// signHex computes the hex-encoded HMAC of body under secret using newHash.
func signHex(t *testing.T, newHash func() hash.Hash, secret, body string) string {
	t.Helper()
	mac := hmac.New(newHash, []byte(secret))
	_, err := mac.Write([]byte(body))
	require.NoError(t, err)
	return hex.EncodeToString(mac.Sum(nil))
}

// failingReader is an io.Reader that always errors. It is used to prove that
// requests rejected on signature length are never read for a body, since
// that rejection must happen before any body buffering occurs.
type failingReader struct{}

func (failingReader) Read([]byte) (int, error) {
	return 0, errors.New("body should not have been read")
}

// hmacNextHandlerSpy is an http.Handler that records whether it was called
// and captures the full request body as observed by the downstream handler.
type hmacNextHandlerSpy struct {
	called   bool
	bodyRead []byte
}

func (s *hmacNextHandlerSpy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.called = true
	b, err := io.ReadAll(r.Body)
	if err == nil {
		s.bodyRead = b
	}
	w.WriteHeader(http.StatusOK)
}

func TestHMACMiddlewareConstructor(t *testing.T) {
	for _, test := range []struct {
		name          string
		secret        string
		header        string
		algorithm     string
		maxBodySize   int
		injectLicense bool
		errContains   string
	}{
		{
			name:          "valid sha256",
			secret:        "topsecret",
			header:        "X-Signature",
			algorithm:     "sha256",
			maxBodySize:   defaultTestMaxBodySize,
			injectLicense: true,
		},
		{
			name:          "valid sha512",
			secret:        "topsecret",
			header:        "X-Signature",
			algorithm:     "sha512",
			maxBodySize:   defaultTestMaxBodySize,
			injectLicense: true,
		},
		{
			name:          "empty secret",
			secret:        "",
			header:        "X-Signature",
			algorithm:     "sha512",
			maxBodySize:   defaultTestMaxBodySize,
			injectLicense: true,
			errContains:   "non-empty secret",
		},
		{
			name:          "empty header",
			secret:        "topsecret",
			header:        "",
			algorithm:     "sha512",
			maxBodySize:   defaultTestMaxBodySize,
			injectLicense: true,
			errContains:   "non-empty header name",
		},
		{
			name:          "unsupported algorithm",
			secret:        "topsecret",
			header:        "X-Signature",
			algorithm:     "md5",
			maxBodySize:   defaultTestMaxBodySize,
			injectLicense: true,
			errContains:   "is not supported",
		},
		{
			name:          "missing enterprise license",
			secret:        "topsecret",
			header:        "X-Signature",
			algorithm:     "sha512",
			maxBodySize:   defaultTestMaxBodySize,
			injectLicense: false,
			errContains:   "requires a valid license",
		},
		{
			name:          "zero max body size",
			secret:        "topsecret",
			header:        "X-Signature",
			algorithm:     "sha512",
			maxBodySize:   0,
			injectLicense: true,
			errContains:   "positive max body size",
		},
		{
			name:          "negative max body size",
			secret:        "topsecret",
			header:        "X-Signature",
			algorithm:     "sha512",
			maxBodySize:   -1,
			injectLicense: true,
			errContains:   "positive max body size",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			mgr := service.MockResources()
			if test.injectLicense {
				license.InjectTestService(mgr)
			}

			m, err := gateway.NewHMACMiddleware(mgr, gateway.HMACConfig{
				Secret:      test.secret,
				Header:      test.header,
				Algorithm:   test.algorithm,
				MaxBodySize: test.maxBodySize,
			})
			if test.errContains != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), test.errContains)
				assert.Nil(t, m)
			} else {
				require.NoError(t, err)
				assert.NotNil(t, m)
			}
		})
	}
}

func newTestHMACMiddleware(t *testing.T, secret, headerName, algorithm string) *gateway.HMACMiddleware {
	t.Helper()
	return newTestHMACMiddlewareWithMaxBodySize(t, secret, headerName, algorithm, defaultTestMaxBodySize)
}

func newTestHMACMiddlewareWithMaxBodySize(t *testing.T, secret, headerName, algorithm string, maxBodySize int) *gateway.HMACMiddleware {
	t.Helper()
	return newTestHMACMiddlewareFromConfig(t, gateway.HMACConfig{
		Secret:      secret,
		Header:      headerName,
		Algorithm:   algorithm,
		MaxBodySize: maxBodySize,
	})
}

// newTestHMACMiddlewareWithPrefix builds a middleware with an explicit
// signature prefix, for tests exercising the prefix-stripping behavior of
// Wrap.
func newTestHMACMiddlewareWithPrefix(t *testing.T, secret, headerName, algorithm, prefix string) *gateway.HMACMiddleware {
	t.Helper()
	return newTestHMACMiddlewareFromConfig(t, gateway.HMACConfig{
		Secret:      secret,
		Header:      headerName,
		Algorithm:   algorithm,
		Prefix:      prefix,
		MaxBodySize: defaultTestMaxBodySize,
	})
}

func newTestHMACMiddlewareFromConfig(t *testing.T, conf gateway.HMACConfig) *gateway.HMACMiddleware {
	t.Helper()
	mgr := service.MockResources()
	license.InjectTestService(mgr)
	m, err := gateway.NewHMACMiddleware(mgr, conf)
	require.NoError(t, err)
	return m
}

func TestHMACMiddlewareWrapHappyPathSHA512(t *testing.T) {
	const secret = "topsecret"
	const headerName = "X-Tfc-Task-Signature"
	const bodyStr = `{"hello":"world"}`

	m := newTestHMACMiddleware(t, secret, headerName, "sha512")

	spy := &hmacNextHandlerSpy{}
	handler := m.Wrap(spy)

	req := httptest.NewRequest(http.MethodPost, "/test", strings.NewReader(bodyStr))
	req.Header.Set(headerName, signHex(t, sha512.New, secret, bodyStr))
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.True(t, spy.called)
	assert.Equal(t, bodyStr, string(spy.bodyRead))
}

func TestHMACMiddlewareWrapHappyPathSHA256(t *testing.T) {
	const secret = "topsecret"
	const headerName = "X-Tfc-Task-Signature"
	const bodyStr = `{"hello":"world"}`

	m := newTestHMACMiddleware(t, secret, headerName, "sha256")

	spy := &hmacNextHandlerSpy{}
	handler := m.Wrap(spy)

	req := httptest.NewRequest(http.MethodPost, "/test", strings.NewReader(bodyStr))
	req.Header.Set(headerName, signHex(t, sha256.New, secret, bodyStr))
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.True(t, spy.called)
	assert.Equal(t, bodyStr, string(spy.bodyRead))
}

func TestHMACMiddlewareWrapMissingSignatureHeader(t *testing.T) {
	m := newTestHMACMiddleware(t, "topsecret", "X-Tfc-Task-Signature", "sha512")

	spy := &hmacNextHandlerSpy{}
	handler := m.Wrap(spy)

	req := httptest.NewRequest(http.MethodPost, "/test", strings.NewReader(`{"hello":"world"}`))
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusUnauthorized, rec.Code)
	assert.False(t, spy.called)
}

func TestHMACMiddlewareWrapNonHexSignature(t *testing.T) {
	m := newTestHMACMiddleware(t, "topsecret", "X-Tfc-Task-Signature", "sha512")

	spy := &hmacNextHandlerSpy{}
	handler := m.Wrap(spy)

	req := httptest.NewRequest(http.MethodPost, "/test", strings.NewReader(`{"hello":"world"}`))
	req.Header.Set("X-Tfc-Task-Signature", "not-valid-hex!!")
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusUnauthorized, rec.Code)
	assert.False(t, spy.called)
}

func TestHMACMiddlewareWrapSignatureLengthMismatch(t *testing.T) {
	const secret = "topsecret"
	const headerName = "X-Tfc-Task-Signature"
	const bodyStr = `{"hello":"world"}`

	// A correctly signed signature for bodyStr, used to derive a truncated
	// (but still valid hex) signature below.
	fullSig := signHex(t, sha512.New, secret, bodyStr)

	for _, test := range []struct {
		name      string
		signature string
	}{
		{
			// Valid hex, correctly computed against the right secret and
			// body, but cut in half so its decoded length no longer matches
			// the sha512 MAC size.
			name:      "truncated correctly-signed signature",
			signature: fullSig[:len(fullSig)/2],
		},
		{
			// Valid hex, far shorter than any supported MAC size.
			name:      "short valid hex",
			signature: "deadbeef",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			m := newTestHMACMiddleware(t, secret, headerName, "sha512")

			spy := &hmacNextHandlerSpy{}
			handler := m.Wrap(spy)

			// The body is a failingReader: if the middleware attempted to
			// read it before rejecting on length, the test would fail with
			// a 400 or an unexpected error path instead of a clean 401.
			req := httptest.NewRequest(http.MethodPost, "/test", failingReader{})
			req.Header.Set(headerName, test.signature)
			rec := httptest.NewRecorder()

			handler.ServeHTTP(rec, req)

			assert.Equal(t, http.StatusUnauthorized, rec.Code)
			assert.False(t, spy.called)
		})
	}
}

func TestHMACMiddlewareWrapWrongSignature(t *testing.T) {
	const secret = "topsecret"
	const headerName = "X-Tfc-Task-Signature"
	const bodyStr = `{"hello":"world"}`

	m := newTestHMACMiddleware(t, secret, headerName, "sha512")

	spy := &hmacNextHandlerSpy{}
	handler := m.Wrap(spy)

	req := httptest.NewRequest(http.MethodPost, "/test", strings.NewReader(bodyStr))
	// Valid hex, but signs a different body than what's sent.
	req.Header.Set(headerName, signHex(t, sha512.New, secret, "not the real body"))
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusUnauthorized, rec.Code)
	assert.False(t, spy.called)
}

func TestHMACMiddlewareWrapWrongSecret(t *testing.T) {
	const headerName = "X-Tfc-Task-Signature"
	const bodyStr = `{"hello":"world"}`

	m := newTestHMACMiddleware(t, "topsecret", headerName, "sha512")

	spy := &hmacNextHandlerSpy{}
	handler := m.Wrap(spy)

	req := httptest.NewRequest(http.MethodPost, "/test", strings.NewReader(bodyStr))
	req.Header.Set(headerName, signHex(t, sha512.New, "wrong-secret", bodyStr))
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusUnauthorized, rec.Code)
	assert.False(t, spy.called)
}

func TestHMACMiddlewareWrapEmptyBody(t *testing.T) {
	const secret = "topsecret"
	const headerName = "X-Tfc-Task-Signature"

	m := newTestHMACMiddleware(t, secret, headerName, "sha512")

	spy := &hmacNextHandlerSpy{}
	handler := m.Wrap(spy)

	req := httptest.NewRequest(http.MethodPost, "/test", strings.NewReader(""))
	req.Header.Set(headerName, signHex(t, sha512.New, secret, ""))
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.True(t, spy.called)
	assert.Empty(t, spy.bodyRead)
}

func TestHMACMiddlewareWrapNilMiddleware(t *testing.T) {
	var m *gateway.HMACMiddleware

	spy := &hmacNextHandlerSpy{}
	handler := m.Wrap(spy)

	req := httptest.NewRequest(http.MethodPost, "/test", strings.NewReader(`{"hello":"world"}`))
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.True(t, spy.called)
}

func TestHMACMiddlewareWrapBodyExceedsMaxSize(t *testing.T) {
	const secret = "topsecret"
	const headerName = "X-Tfc-Task-Signature"
	const maxBodySize = 16
	bodyStr := strings.Repeat("a", 64)

	m := newTestHMACMiddlewareWithMaxBodySize(t, secret, headerName, "sha512", maxBodySize)

	spy := &hmacNextHandlerSpy{}
	handler := m.Wrap(spy)

	req := httptest.NewRequest(http.MethodPost, "/test", strings.NewReader(bodyStr))
	// Correctly signed, but the body itself is larger than the configured cap.
	req.Header.Set(headerName, signHex(t, sha512.New, secret, bodyStr))
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusRequestEntityTooLarge, rec.Code)
	assert.False(t, spy.called)
}

func TestHMACMiddlewareWrapBodyAtMaxSizeIsAllowed(t *testing.T) {
	const secret = "topsecret"
	const headerName = "X-Tfc-Task-Signature"
	const maxBodySize = 16
	bodyStr := strings.Repeat("a", maxBodySize)

	m := newTestHMACMiddlewareWithMaxBodySize(t, secret, headerName, "sha512", maxBodySize)

	spy := &hmacNextHandlerSpy{}
	handler := m.Wrap(spy)

	req := httptest.NewRequest(http.MethodPost, "/test", strings.NewReader(bodyStr))
	req.Header.Set(headerName, signHex(t, sha512.New, secret, bodyStr))
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	// http.MaxBytesReader permits reading exactly n bytes; only bodies
	// strictly larger than the cap are rejected.
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.True(t, spy.called)
	assert.Equal(t, bodyStr, string(spy.bodyRead))
}

func TestHMACMiddlewareWrapHeaderLookupIsCaseInsensitive(t *testing.T) {
	const secret = "topsecret"
	const headerName = "X-Tfc-Task-Signature"
	const bodyStr = `{"hello":"world"}`

	m := newTestHMACMiddleware(t, secret, headerName, "sha512")

	spy := &hmacNextHandlerSpy{}
	handler := m.Wrap(spy)

	req := httptest.NewRequest(http.MethodPost, "/test", strings.NewReader(bodyStr))
	// Set the header using a different casing than the configured header
	// name; net/http canonicalizes header names so this must still match.
	req.Header.Set("x-tfc-task-signature", signHex(t, sha512.New, secret, bodyStr))
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.True(t, spy.called)
	assert.Equal(t, bodyStr, string(spy.bodyRead))
}

func TestHMACMiddlewareWrapPrefix(t *testing.T) {
	const secret = "topsecret"
	const headerName = "X-Hub-Signature-256"
	const bodyStr = `{"hello":"world"}`

	correctSig := signHex(t, sha256.New, secret, bodyStr)

	for _, test := range []struct {
		name             string
		configuredPrefix string
		headerValue      string
		wantStatus       int
	}{
		{
			// GitHub/Meta-style: the configured prefix matches the one sent,
			// so it is stripped and the remaining hex verifies correctly.
			name:             "prefix configured and header matches",
			configuredPrefix: "sha256=",
			headerValue:      "sha256=" + correctSig,
			wantStatus:       http.StatusOK,
		},
		{
			// A correctly-signed, bare-hex signature is rejected when a
			// prefix is configured but the header does not carry it.
			name:             "prefix configured but header omits it",
			configuredPrefix: "sha256=",
			headerValue:      correctSig,
			wantStatus:       http.StatusUnauthorized,
		},
		{
			// With no prefix configured, CutPrefix trivially matches and the
			// full header value (including the literal "sha256=") is passed
			// to hex decoding, which fails since it is not valid hex.
			name:             "no prefix configured but header includes one",
			configuredPrefix: "",
			headerValue:      "sha256=" + correctSig,
			wantStatus:       http.StatusUnauthorized,
		},
		{
			// The header carries a different fixed prefix than the one
			// configured, so CutPrefix does not match.
			name:             "wrong prefix sent",
			configuredPrefix: "sha256=",
			headerValue:      "sha512=" + correctSig,
			wantStatus:       http.StatusUnauthorized,
		},
		{
			// The header value is exactly the configured prefix, leaving an
			// empty signature after stripping; this is valid hex (zero
			// bytes) but fails the MAC size check.
			name:             "header equals prefix exactly",
			configuredPrefix: "sha256=",
			headerValue:      "sha256=",
			wantStatus:       http.StatusUnauthorized,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			m := newTestHMACMiddlewareWithPrefix(t, secret, headerName, "sha256", test.configuredPrefix)

			spy := &hmacNextHandlerSpy{}
			handler := m.Wrap(spy)

			req := httptest.NewRequest(http.MethodPost, "/test", strings.NewReader(bodyStr))
			req.Header.Set(headerName, test.headerValue)
			rec := httptest.NewRecorder()

			handler.ServeHTTP(rec, req)

			assert.Equal(t, test.wantStatus, rec.Code)
			if test.wantStatus == http.StatusOK {
				assert.True(t, spy.called)
				assert.Equal(t, bodyStr, string(spy.bodyRead))
			} else {
				assert.False(t, spy.called)
			}
		})
	}
}
