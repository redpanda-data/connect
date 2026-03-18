// Copyright 2025 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md

package catalogx

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/apache/iceberg-go/catalog/rest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIsAuthErr(t *testing.T) {
	tests := []struct {
		name   string
		err    error
		expect bool
	}{
		{"nil", nil, false},
		{"unrelated", fmt.Errorf("something else"), false},
		{"forbidden", rest.ErrForbidden, true},
		{"wrapped forbidden", fmt.Errorf("op failed: %w", rest.ErrForbidden), true},
		{"authorization expired", rest.ErrAuthorizationExpired, true},
		{"wrapped expired", fmt.Errorf("op failed: %w", rest.ErrAuthorizationExpired), true},
		{"bad request", rest.ErrBadRequest, false},
		{"server error", rest.ErrServerError, false},
		{"unauthorized", rest.ErrUnauthorized, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.expect, isAuthErr(tc.err))
		})
	}
}

// mockRESTServer wraps httptest.Server and always handles /v1/config (required
// by rest.NewCatalog on construction). All other paths are dispatched to the
// caller-provided handler.
type mockRESTServer struct {
	*httptest.Server
	configCalls atomic.Int32
}

func newMockRESTServer(handler http.HandlerFunc) *mockRESTServer {
	m := &mockRESTServer{}
	m.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/config" {
			m.configCalls.Add(1)
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{"defaults": map[string]any{}, "overrides": map[string]any{}})
			return
		}
		handler(w, r)
	}))
	return m
}

func newTestClient(t *testing.T, serverURL string, namespace []string) *Client {
	t.Helper()
	client, err := NewCatalogClient(t.Context(), Config{
		URL:      serverURL,
		AuthType: "none",
	}, namespace)
	require.NoError(t, err)
	return client
}

func TestLoadTableRetryOnAuthErr(t *testing.T) {
	var calls atomic.Int32
	srv := newMockRESTServer(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, "/tables/") {
			n := calls.Add(1)
			if n == 1 {
				w.WriteHeader(http.StatusForbidden)
				return
			}
			// Return 404 on retry to prove the retry happened
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.WriteHeader(http.StatusNotFound)
	})
	defer srv.Close()

	client := newTestClient(t, srv.URL, []string{"testns"})
	_, err := client.LoadTable(context.Background(), "my_table")
	require.Error(t, err)
	// The error should NOT be an auth error — it should be the 404 from the retry
	assert.False(t, isAuthErr(err), "after retry, error should not be auth-related")
	assert.Equal(t, int32(2), calls.Load(), "expected exactly 2 calls (1 auth fail + 1 retry)")
}

func TestLoadTableNoRetryOnNonAuthErr(t *testing.T) {
	var calls atomic.Int32
	srv := newMockRESTServer(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusNotFound)
		_ = json.NewEncoder(w).Encode(map[string]any{"error": map[string]any{"message": "not found", "type": "NoSuchTableException", "code": 404}})
	})
	defer srv.Close()

	client := newTestClient(t, srv.URL, []string{"testns"})
	_, err := client.LoadTable(context.Background(), "missing_table")
	require.Error(t, err)
	assert.Equal(t, int32(1), calls.Load(), "should not retry on non-auth error")
}

func TestCheckTableExistsRetryOnAuthErr(t *testing.T) {
	var calls atomic.Int32
	srv := newMockRESTServer(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodHead && strings.Contains(r.URL.Path, "/tables/") {
			n := calls.Add(1)
			if n == 1 {
				w.WriteHeader(http.StatusForbidden)
				return
			}
			w.WriteHeader(http.StatusNoContent)
			return
		}
		w.WriteHeader(http.StatusNotFound)
	})
	defer srv.Close()

	client := newTestClient(t, srv.URL, []string{"ns"})
	exists, err := client.CheckTableExists(context.Background(), "tbl")
	require.NoError(t, err)
	assert.True(t, exists)
	assert.Equal(t, int32(2), calls.Load())
}

func TestCreateNamespaceRetryOnAuthErr(t *testing.T) {
	var calls atomic.Int32
	srv := newMockRESTServer(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost && r.URL.Path == "/v1/namespaces" {
			n := calls.Add(1)
			if n == 1 {
				w.WriteHeader(http.StatusForbidden)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			_ = json.NewEncoder(w).Encode(map[string]any{"namespace": []string{"myns"}, "properties": map[string]any{}})
			return
		}
		w.WriteHeader(http.StatusNotFound)
	})
	defer srv.Close()

	client := newTestClient(t, srv.URL, []string{"myns"})
	err := client.CreateNamespace(context.Background(), nil)
	require.NoError(t, err)
	assert.Equal(t, int32(2), calls.Load())
}

func TestCheckNamespaceExistsRetryOnAuthErr(t *testing.T) {
	var calls atomic.Int32
	srv := newMockRESTServer(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodHead && r.URL.Path == "/v1/namespaces/myns" {
			n := calls.Add(1)
			if n == 1 {
				w.WriteHeader(http.StatusForbidden)
				return
			}
			w.WriteHeader(http.StatusNoContent)
			return
		}
		w.WriteHeader(http.StatusNotFound)
	})
	defer srv.Close()

	client := newTestClient(t, srv.URL, []string{"myns"})
	exists, err := client.CheckNamespaceExists(context.Background())
	require.NoError(t, err)
	assert.True(t, exists)
	assert.Equal(t, int32(2), calls.Load())
}

func TestConcurrentRefreshCatalog(t *testing.T) {
	var tableCalls atomic.Int32
	srv := newMockRESTServer(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodHead && strings.Contains(r.URL.Path, "/tables/") {
			n := tableCalls.Add(1)
			if n <= 5 {
				w.WriteHeader(http.StatusForbidden)
				return
			}
			w.WriteHeader(http.StatusNoContent)
			return
		}
		w.WriteHeader(http.StatusNotFound)
	})
	defer srv.Close()

	client := newTestClient(t, srv.URL, []string{"ns"})

	const goroutines = 10
	var wg sync.WaitGroup
	errs := make([]error, goroutines)
	wg.Add(goroutines)
	for i := range goroutines {
		go func(idx int) {
			defer wg.Done()
			_, errs[idx] = client.CheckTableExists(context.Background(), "tbl")
		}(i)
	}
	wg.Wait()

	for i, err := range errs {
		assert.NoError(t, err, "goroutine %d failed", i)
	}
	assert.GreaterOrEqual(t, srv.configCalls.Load(), int32(2), "expected at least one catalog refresh")
}
