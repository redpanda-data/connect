// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package gateway_test

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"crypto/sha512"
	"encoding/hex"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/mux"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/redpanda-data/connect/v4/internal/impl/gateway"
	"github.com/redpanda-data/connect/v4/internal/license"
)

func sha512HexSignature(t *testing.T, secret, body string) string {
	t.Helper()
	mac := hmac.New(sha512.New, []byte(secret))
	_, err := mac.Write([]byte(body))
	require.NoError(t, err)
	return hex.EncodeToString(mac.Sum(nil))
}

func sha256HexSignature(t *testing.T, secret, body string) string {
	t.Helper()
	mac := hmac.New(sha256.New, []byte(secret))
	_, err := mac.Write([]byte(body))
	require.NoError(t, err)
	return hex.EncodeToString(mac.Sum(nil))
}

// drainOneBatch reads a single batch from the input and acknowledges it with
// no error, in a background goroutine, for the duration of tCtx.
func drainOneBatch(t *testing.T, tCtx context.Context, h *gateway.Input) {
	t.Helper()
	go func() {
		batch, aFn, err := h.ReadBatch(tCtx)
		if err != nil {
			return
		}
		_ = aFn(tCtx, nil)
		_ = batch
	}()
}

func TestGatewayInputHMACConfigEndToEnd(t *testing.T) {
	t.Setenv("REDPANDA_CLOUD_GATEWAY_ADDRESS", "0.0.0.0:1234")

	tCtx, done := context.WithTimeout(t.Context(), 30*time.Second)
	defer done()

	const secret = "topsecret"

	pConf, err := gateway.InputSpec().ParseYAML(`
path: /testpost
auth:
  hmac:
    secret: topsecret
    header: X-Tfc-Task-Signature
    algorithm: sha512
`, nil)
	require.NoError(t, err)

	mgr := service.MockResources()
	license.InjectTestService(mgr)

	h, err := gateway.InputFromParsed(pConf, mgr)
	require.NoError(t, err)

	router := mux.NewRouter()
	require.NoError(t, h.RegisterCustomMux(router))

	server := httptest.NewServer(router)
	defer server.Close()

	t.Run("signed request passes", func(t *testing.T) {
		const bodyStr = `{"hello":"world"}`
		drainOneBatch(t, tCtx, h)

		req, err := http.NewRequestWithContext(tCtx, http.MethodPost, server.URL+"/testpost", strings.NewReader(bodyStr))
		require.NoError(t, err)
		req.Header.Set("X-Tfc-Task-Signature", sha512HexSignature(t, secret, bodyStr))

		res, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		defer res.Body.Close()
		assert.Equal(t, http.StatusOK, res.StatusCode)
	})

	t.Run("unsigned request is rejected", func(t *testing.T) {
		const bodyStr = `{"hello":"world"}`

		res, err := http.Post(server.URL+"/testpost", "application/octet-stream", strings.NewReader(bodyStr))
		require.NoError(t, err)
		defer res.Body.Close()
		assert.Equal(t, http.StatusUnauthorized, res.StatusCode)
	})

	t.Run("bad signature request is rejected", func(t *testing.T) {
		const bodyStr = `{"hello":"world"}`

		req, err := http.NewRequestWithContext(tCtx, http.MethodPost, server.URL+"/testpost", strings.NewReader(bodyStr))
		require.NoError(t, err)
		req.Header.Set("X-Tfc-Task-Signature", sha512HexSignature(t, "wrong-secret", bodyStr))

		res, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		defer res.Body.Close()
		assert.Equal(t, http.StatusUnauthorized, res.StatusCode)
	})
}

// TestGatewayInputHMACPrefixEndToEnd exercises the GitHub-style
// `X-Hub-Signature-256: sha256=<hex>` convention end-to-end through the
// full input: a request signed with the configured prefix must pass, and
// the same request without the prefix must be rejected.
func TestGatewayInputHMACPrefixEndToEnd(t *testing.T) {
	t.Setenv("REDPANDA_CLOUD_GATEWAY_ADDRESS", "0.0.0.0:1234")

	tCtx, done := context.WithTimeout(t.Context(), 30*time.Second)
	defer done()

	const secret = "topsecret"

	pConf, err := gateway.InputSpec().ParseYAML(`
path: /testpost
auth:
  hmac:
    secret: topsecret
    header: X-Hub-Signature-256
    algorithm: sha256
    prefix: "sha256="
`, nil)
	require.NoError(t, err)

	mgr := service.MockResources()
	license.InjectTestService(mgr)

	h, err := gateway.InputFromParsed(pConf, mgr)
	require.NoError(t, err)

	router := mux.NewRouter()
	require.NoError(t, h.RegisterCustomMux(router))

	server := httptest.NewServer(router)
	defer server.Close()

	t.Run("prefixed signature passes", func(t *testing.T) {
		const bodyStr = `{"hello":"world"}`
		drainOneBatch(t, tCtx, h)

		req, err := http.NewRequestWithContext(tCtx, http.MethodPost, server.URL+"/testpost", strings.NewReader(bodyStr))
		require.NoError(t, err)
		req.Header.Set("X-Hub-Signature-256", "sha256="+sha256HexSignature(t, secret, bodyStr))

		res, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		defer res.Body.Close()
		assert.Equal(t, http.StatusOK, res.StatusCode)
	})

	t.Run("signature without prefix is rejected", func(t *testing.T) {
		const bodyStr = `{"hello":"world"}`

		req, err := http.NewRequestWithContext(tCtx, http.MethodPost, server.URL+"/testpost", strings.NewReader(bodyStr))
		require.NoError(t, err)
		req.Header.Set("X-Hub-Signature-256", sha256HexSignature(t, secret, bodyStr))

		res, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		defer res.Body.Close()
		assert.Equal(t, http.StatusUnauthorized, res.StatusCode)
	})
}

func TestGatewayInputHMACMaxBodySizeEndToEnd(t *testing.T) {
	t.Setenv("REDPANDA_CLOUD_GATEWAY_ADDRESS", "0.0.0.0:1234")

	tCtx, done := context.WithTimeout(t.Context(), 30*time.Second)
	defer done()

	const secret = "topsecret"

	pConf, err := gateway.InputSpec().ParseYAML(`
path: /testpost
auth:
  hmac:
    secret: topsecret
    header: X-Tfc-Task-Signature
    algorithm: sha512
    max_body_size: 32
`, nil)
	require.NoError(t, err)

	mgr := service.MockResources()
	license.InjectTestService(mgr)

	h, err := gateway.InputFromParsed(pConf, mgr)
	require.NoError(t, err)

	router := mux.NewRouter()
	require.NoError(t, h.RegisterCustomMux(router))

	server := httptest.NewServer(router)
	defer server.Close()

	t.Run("oversized signed request is rejected", func(t *testing.T) {
		bodyStr := strings.Repeat("a", 64)

		req, err := http.NewRequestWithContext(tCtx, http.MethodPost, server.URL+"/testpost", strings.NewReader(bodyStr))
		require.NoError(t, err)
		req.Header.Set("X-Tfc-Task-Signature", sha512HexSignature(t, secret, bodyStr))

		res, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		defer res.Body.Close()
		assert.Equal(t, http.StatusRequestEntityTooLarge, res.StatusCode)
	})

	t.Run("small signed request passes", func(t *testing.T) {
		const bodyStr = `{"hello":"world"}`
		drainOneBatch(t, tCtx, h)

		req, err := http.NewRequestWithContext(tCtx, http.MethodPost, server.URL+"/testpost", strings.NewReader(bodyStr))
		require.NoError(t, err)
		req.Header.Set("X-Tfc-Task-Signature", sha512HexSignature(t, secret, bodyStr))

		res, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		defer res.Body.Close()
		assert.Equal(t, http.StatusOK, res.StatusCode)
	})
}

func TestGatewayInputWithoutHMACConfigBehavesAsBefore(t *testing.T) {
	t.Setenv("REDPANDA_CLOUD_GATEWAY_ADDRESS", "0.0.0.0:1234")

	tCtx, done := context.WithTimeout(t.Context(), 30*time.Second)
	defer done()

	pConf, err := gateway.InputSpec().ParseYAML(`
path: /testpost
`, nil)
	require.NoError(t, err)

	// No license injected: absence of an hmac block must not require an
	// enterprise license, matching pre-HMAC behavior.
	h, err := gateway.InputFromParsed(pConf, service.MockResources())
	require.NoError(t, err)

	router := mux.NewRouter()
	require.NoError(t, h.RegisterCustomMux(router))

	server := httptest.NewServer(router)
	defer server.Close()

	drainOneBatch(t, tCtx, h)

	// No JWT env vars are set, so the JWT middleware is a no-op and this
	// plain, unsigned request must reach the handler as it did before HMAC
	// support was added.
	res, err := http.Post(server.URL+"/testpost", "application/octet-stream", strings.NewReader("plain body"))
	require.NoError(t, err)
	defer res.Body.Close()
	assert.Equal(t, http.StatusOK, res.StatusCode)
}

// TestGatewayInputHMACBypassesPlatformJWT verifies that when auth.hmac is
// enabled, it fully replaces the platform-managed JWT/RBAC authentication:
// a request signed with the HMAC secret and no Authorization header at all
// still succeeds even though the platform JWT env vars are configured, and
// an unsigned request is still rejected.
func TestGatewayInputHMACBypassesPlatformJWT(t *testing.T) {
	t.Setenv("REDPANDA_CLOUD_GATEWAY_ADDRESS", "0.0.0.0:1234")
	// JWKS fetching is lazy, so a fake issuer URL is fine here: the point of
	// this test is that the JWT validator is never constructed at all when
	// auth.hmac is enabled, so this URL is never dialed.
	t.Setenv("REDPANDA_CLOUD_GATEWAY_JWT_ISSUER_URL", "https://127.0.0.1:1/")
	t.Setenv("REDPANDA_CLOUD_GATEWAY_JWT_AUDIENCE", "test-audience")
	t.Setenv("REDPANDA_CLOUD_GATEWAY_JWT_ORGANIZATION_ID", "test-org")

	tCtx, done := context.WithTimeout(t.Context(), 30*time.Second)
	defer done()

	const secret = "topsecret"

	pConf, err := gateway.InputSpec().ParseYAML(`
path: /testpost
auth:
  hmac:
    secret: topsecret
    header: X-Tfc-Task-Signature
    algorithm: sha512
`, nil)
	require.NoError(t, err)

	mgr := service.MockResources()
	license.InjectTestService(mgr)

	h, err := gateway.InputFromParsed(pConf, mgr)
	require.NoError(t, err)

	router := mux.NewRouter()
	require.NoError(t, h.RegisterCustomMux(router))

	server := httptest.NewServer(router)
	defer server.Close()

	t.Run("signed request without Authorization header passes", func(t *testing.T) {
		const bodyStr = `{"hello":"world"}`
		drainOneBatch(t, tCtx, h)

		req, err := http.NewRequestWithContext(tCtx, http.MethodPost, server.URL+"/testpost", strings.NewReader(bodyStr))
		require.NoError(t, err)
		req.Header.Set("X-Tfc-Task-Signature", sha512HexSignature(t, secret, bodyStr))
		require.Empty(t, req.Header.Get("Authorization"))

		res, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		defer res.Body.Close()
		assert.Equal(t, http.StatusOK, res.StatusCode)
	})

	t.Run("unsigned request is still rejected", func(t *testing.T) {
		const bodyStr = `{"hello":"world"}`

		res, err := http.Post(server.URL+"/testpost", "application/octet-stream", strings.NewReader(bodyStr))
		require.NoError(t, err)
		defer res.Body.Close()
		assert.Equal(t, http.StatusUnauthorized, res.StatusCode)
	})
}

func TestGatewayInputHMACInvalidAlgorithm(t *testing.T) {
	t.Setenv("REDPANDA_CLOUD_GATEWAY_ADDRESS", "0.0.0.0:1234")

	// The enum field does not reject the bad value at ParseYAML time, only
	// via linting, so parsing succeeds here.
	pConf, err := gateway.InputSpec().ParseYAML(`
path: /testpost
auth:
  hmac:
    secret: topsecret
    header: X-Tfc-Task-Signature
    algorithm: md5
`, nil)
	require.NoError(t, err)

	// Confirm the linter flags the invalid enum value.
	linter := service.NewEnvironment().NewComponentConfigLinter()
	lints, err := linter.LintInputYAML([]byte(`
gateway:
  path: /testpost
  auth:
    hmac:
      secret: topsecret
      header: X-Tfc-Task-Signature
      algorithm: md5
`))
	require.NoError(t, err)
	require.Len(t, lints, 1)
	assert.Contains(t, lints[0].Error(), "not a valid option")

	// Construction is the enforcement point: an unsupported algorithm must
	// fail here even though ParseYAML let it through.
	mgr := service.MockResources()
	license.InjectTestService(mgr)

	_, err = gateway.InputFromParsed(pConf, mgr)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "is not supported")
}

func TestGatewayInputHMACLintEmptySecret(t *testing.T) {
	linter := service.NewEnvironment().NewComponentConfigLinter()
	lints, err := linter.LintInputYAML([]byte(`
gateway:
  path: /testpost
  auth:
    hmac:
      secret: ""
      header: X-Tfc-Task-Signature
      algorithm: sha512
`))
	require.NoError(t, err)
	require.Len(t, lints, 1)
	assert.Contains(t, lints[0].Error(), "a non-empty secret is required")
}

func TestGatewayInputHMACLintNonPositiveMaxBodySize(t *testing.T) {
	linter := service.NewEnvironment().NewComponentConfigLinter()
	lints, err := linter.LintInputYAML([]byte(`
gateway:
  path: /testpost
  auth:
    hmac:
      secret: topsecret
      header: X-Tfc-Task-Signature
      algorithm: sha512
      max_body_size: 0
`))
	require.NoError(t, err)
	require.Len(t, lints, 1)
	assert.Contains(t, lints[0].Error(), "max_body_size must be greater than zero")
}

func TestGatewayInputHMACMissingHeaderFailsToParse(t *testing.T) {
	t.Setenv("REDPANDA_CLOUD_GATEWAY_ADDRESS", "0.0.0.0:1234")

	// `header` has no default, so an hmac block without it must fail to
	// parse rather than falling back to a default header name.
	_, err := gateway.InputSpec().ParseYAML(`
path: /testpost
auth:
  hmac:
    secret: topsecret
    algorithm: sha512
`, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "header")
}
