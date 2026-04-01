// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package salesforcehttp

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestUpdateAndSetBearerTokenRealClient(t *testing.T) {
	t.Parallel()

	// Fake Salesforce OAuth server
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/services/oauth2/token", r.URL.Path)

		resp := map[string]string{"access_token": "abc123"}
		_ = json.NewEncoder(w).Encode(resp)
	}))
	defer ts.Close()

	client, err := NewClient(ClientConfig{
		OrgURL:         ts.URL,
		ClientID:       "id",
		ClientSecret:   "secret",
		APIVersion:     "v65.0",
		QueryBatchSize: 2000,
		HTTPClient:     ts.Client(),
	})
	require.NoError(t, err)

	err = client.updateAndSetBearerToken(t.Context())
	require.NoError(t, err)
	assert.Equal(t, "abc123", client.getBearerToken())
}

func TestCallSalesforceAPIRefreshOn401RealClient(t *testing.T) {
	t.Parallel()

	callCount := 0
	tokenIssued := false

	// Fake Salesforce server
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {

		// Token refresh endpoint
		case "/services/oauth2/token":
			tokenIssued = true
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"access_token":"new-token"}`))
			return

		// Data endpoint
		case "/services/data/v65.0":
			callCount++

			// First call → 401 Unauthorized
			if callCount == 1 {
				w.WriteHeader(http.StatusUnauthorized)
				return
			}

			// Second call → success
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"ok":true}`))
			return

		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer ts.Close()

	client, err := NewClient(ClientConfig{
		OrgURL:         ts.URL,
		ClientID:       "id",
		ClientSecret:   "secret",
		APIVersion:     "v65.0",
		QueryBatchSize: 2000,
		HTTPClient:     ts.Client(),
	})
	require.NoError(t, err)

	body, err := client.callSalesforceAPI(t.Context(), mustParseURL(ts.URL+"/services/data/v65.0"))
	require.NoError(t, err)

	assert.Equal(t, `{"ok":true}`, string(body))
	assert.Equal(t, 2, callCount)
	assert.True(t, tokenIssued, "token refresh should have been called")
}

func mustParseURL(s string) *url.URL {
	u, _ := url.Parse(s)
	return u
}

func TestDo(t *testing.T) {
	t.Run("2xxReturnsBody", func(t *testing.T) {
		t.Parallel()

		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"ok":true}`))
		}))
		defer ts.Close()

		client, err := NewClient(ClientConfig{
			OrgURL:         ts.URL,
			ClientID:       "id",
			ClientSecret:   "secret",
			APIVersion:     "v65.0",
			QueryBatchSize: 2000,
			HTTPClient:     ts.Client(),
		})
		require.NoError(t, err)

		req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, ts.URL+"/services/data/v65.0", nil)
		require.NoError(t, err)

		body, err := client.do(req)
		require.NoError(t, err)
		assert.Equal(t, `{"ok":true}`, string(body))
	})

	t.Run("Non2xxReturnsHTTPError", func(t *testing.T) {
		t.Parallel()

		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("X-Salesforce-Error", "session-expired")
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`not found`))
		}))
		defer ts.Close()

		client, err := NewClient(ClientConfig{
			OrgURL:         ts.URL,
			ClientID:       "id",
			ClientSecret:   "secret",
			APIVersion:     "v65.0",
			QueryBatchSize: 2000,
			HTTPClient:     ts.Client(),
		})
		require.NoError(t, err)

		req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, ts.URL+"/services/data/v65.0", nil)
		require.NoError(t, err)

		_, err = client.do(req)
		require.Error(t, err)

		var httpErr *HTTPError
		require.ErrorAs(t, err, &httpErr)
		assert.Equal(t, http.StatusNotFound, httpErr.StatusCode)
		assert.Equal(t, "not found", httpErr.Body)
		assert.Equal(t, "session-expired", httpErr.Headers.Get("X-Salesforce-Error"))
	})

	t.Run("5xxReturnsHTTPError", func(t *testing.T) {
		t.Parallel()

		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte(`server error`))
		}))
		defer ts.Close()

		client, err := NewClient(ClientConfig{
			OrgURL:         ts.URL,
			ClientID:       "id",
			ClientSecret:   "secret",
			APIVersion:     "v65.0",
			QueryBatchSize: 2000,
			HTTPClient:     ts.Client(),
		})
		require.NoError(t, err)

		req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, ts.URL+"/services/data/v65.0", nil)
		require.NoError(t, err)

		_, err = client.do(req)
		require.Error(t, err)

		var httpErr *HTTPError
		require.ErrorAs(t, err, &httpErr)
		assert.Equal(t, http.StatusInternalServerError, httpErr.StatusCode)
		assert.Equal(t, "server error", httpErr.Body)
	})
}

func TestWithAuth(t *testing.T) {
	t.Run("SuccessOnFirstCall", func(t *testing.T) {
		t.Parallel()

		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			t.Error("unexpected HTTP call — token is already set")
		}))
		defer ts.Close()

		client, err := NewClient(ClientConfig{
			OrgURL:         ts.URL,
			ClientID:       "id",
			ClientSecret:   "secret",
			APIVersion:     "v65.0",
			QueryBatchSize: 2000,
			HTTPClient:     ts.Client(),
		})
		require.NoError(t, err)
		client.bearerToken.Store("existing-token")

		calls := 0
		body, err := client.withAuth(t.Context(), func() ([]byte, error) {
			calls++
			return []byte(`{"result":"ok"}`), nil
		})

		require.NoError(t, err)
		assert.Equal(t, `{"result":"ok"}`, string(body))
		assert.Equal(t, 1, calls)
	})

	t.Run("FetchesTokenIfEmpty", func(t *testing.T) {
		t.Parallel()

		tokenIssued := false
		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/services/oauth2/token" {
				tokenIssued = true
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write([]byte(`{"access_token":"fetched-token"}`))
				return
			}
			t.Errorf("unexpected request to %s", r.URL.Path)
		}))
		defer ts.Close()

		client, err := NewClient(ClientConfig{
			OrgURL:         ts.URL,
			ClientID:       "id",
			ClientSecret:   "secret",
			APIVersion:     "v65.0",
			QueryBatchSize: 2000,
			HTTPClient:     ts.Client(),
		})
		require.NoError(t, err)
		assert.Empty(t, client.getBearerToken())

		body, err := client.withAuth(t.Context(), func() ([]byte, error) {
			return []byte(`{"fetched":true}`), nil
		})

		require.NoError(t, err)
		assert.Equal(t, `{"fetched":true}`, string(body))
		assert.True(t, tokenIssued, "expected token endpoint to be called")
		assert.Equal(t, "fetched-token", client.getBearerToken())
	})

	t.Run("401RefreshesAndRetries", func(t *testing.T) {
		t.Parallel()

		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/services/oauth2/token" {
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write([]byte(`{"access_token":"refreshed-token"}`))
				return
			}
			t.Errorf("unexpected request to %s", r.URL.Path)
		}))
		defer ts.Close()

		client, err := NewClient(ClientConfig{
			OrgURL:         ts.URL,
			ClientID:       "id",
			ClientSecret:   "secret",
			APIVersion:     "v65.0",
			QueryBatchSize: 2000,
			HTTPClient:     ts.Client(),
		})
		require.NoError(t, err)
		client.bearerToken.Store("stale-token")

		calls := 0
		body, err := client.withAuth(t.Context(), func() ([]byte, error) {
			calls++
			if calls == 1 {
				return nil, &HTTPError{StatusCode: http.StatusUnauthorized, Reason: "Unauthorized"}
			}
			return []byte(`{"retried":true}`), nil
		})

		require.NoError(t, err)
		assert.Equal(t, `{"retried":true}`, string(body))
		assert.Equal(t, 2, calls)
		assert.Equal(t, "refreshed-token", client.getBearerToken())
	})

	t.Run("401RefreshFails", func(t *testing.T) {
		t.Parallel()

		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/services/oauth2/token" {
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			t.Errorf("unexpected request to %s", r.URL.Path)
		}))
		defer ts.Close()

		client, err := NewClient(ClientConfig{
			OrgURL:         ts.URL,
			ClientID:       "id",
			ClientSecret:   "secret",
			APIVersion:     "v65.0",
			QueryBatchSize: 2000,
			HTTPClient:     ts.Client(),
		})
		require.NoError(t, err)
		client.bearerToken.Store("stale-token")

		_, err = client.withAuth(t.Context(), func() ([]byte, error) {
			return nil, &HTTPError{StatusCode: http.StatusUnauthorized, Reason: "Unauthorized"}
		})

		require.Error(t, err)
		assert.ErrorContains(t, err, "refresh token")
	})

	t.Run("401SecondCallFails", func(t *testing.T) {
		t.Parallel()

		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/services/oauth2/token" {
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write([]byte(`{"access_token":"new-token"}`))
				return
			}
			t.Errorf("unexpected request to %s", r.URL.Path)
		}))
		defer ts.Close()

		client, err := NewClient(ClientConfig{
			OrgURL:         ts.URL,
			ClientID:       "id",
			ClientSecret:   "secret",
			APIVersion:     "v65.0",
			QueryBatchSize: 2000,
			HTTPClient:     ts.Client(),
		})
		require.NoError(t, err)
		client.bearerToken.Store("stale-token")

		calls := 0
		_, err = client.withAuth(t.Context(), func() ([]byte, error) {
			calls++
			if calls == 1 {
				return nil, &HTTPError{StatusCode: http.StatusUnauthorized, Reason: "Unauthorized"}
			}
			return nil, &HTTPError{StatusCode: http.StatusBadRequest, Reason: "Bad Request"}
		})

		require.Error(t, err)
		assert.ErrorContains(t, err, "request failed after token refresh")
		assert.Equal(t, 2, calls)
	})

	t.Run("NonHTTPErrorNotRetried", func(t *testing.T) {
		t.Parallel()

		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			t.Error("unexpected HTTP call — non-HTTP errors should not trigger token refresh")
		}))
		defer ts.Close()

		client, err := NewClient(ClientConfig{
			OrgURL:         ts.URL,
			ClientID:       "id",
			ClientSecret:   "secret",
			APIVersion:     "v65.0",
			QueryBatchSize: 2000,
			HTTPClient:     ts.Client(),
		})
		require.NoError(t, err)
		client.bearerToken.Store("existing-token")

		networkErr := fmt.Errorf("network error")
		calls := 0
		_, err = client.withAuth(t.Context(), func() ([]byte, error) {
			calls++
			return nil, networkErr
		})

		require.ErrorIs(t, err, networkErr)
		assert.Equal(t, 1, calls)
	})
}
