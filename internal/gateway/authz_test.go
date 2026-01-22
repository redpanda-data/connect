// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package gateway_test

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/common-go/authz"
	"github.com/redpanda-data/connect/v4/internal/gateway"
)

const (
	authzTestResourceName authz.ResourceName   = "organization/test-org/resourcegroup/default/dataplane/test-service"
	authzTestPermRead     authz.PermissionName = "test_service_read"
	authzTestPermWrite    authz.PermissionName = "test_service_write"
	authzTestPrincipal    authz.PrincipalID    = "test@example.com"
	authzOtherPrincipal   authz.PrincipalID    = "other@example.com"
)

// testHandler is a simple HTTP handler that writes "OK" on success
var testHandler = http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("OK"))
})

func TestAuthzMiddlewareAllowAll(t *testing.T) {
	t.Log("Given: Policy file granting all permissions")
	policy := setupPolicy(t, "testdata/policies/allow_all.yaml")
	defer policy.Close()

	t.Log("And: Middleware protecting a handler with read permission")
	middleware := gateway.AuthzMiddleware(policy, authzTestPermRead, testHandler)

	t.Log("When: Request with valid principal in context")
	req := httptest.NewRequest(http.MethodGet, "/test", http.NoBody)
	req = req.WithContext(gateway.ContextWithValidatedPrincipalID(req.Context(), authzTestPrincipal))
	rec := httptest.NewRecorder()
	middleware.ServeHTTP(rec, req)

	t.Log("Then: Request succeeds")
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestAuthzMiddlewareDenyAll(t *testing.T) {
	t.Log("Given: Policy file denying all permissions")
	policy := setupPolicy(t, "testdata/policies/deny_all.yaml")
	defer policy.Close()

	t.Log("And: Middleware protecting a handler with read permission")
	middleware := gateway.AuthzMiddleware(policy, authzTestPermRead, testHandler)

	t.Log("When: Request with valid principal but no permissions")
	req := httptest.NewRequest(http.MethodGet, "/test", http.NoBody)
	req = req.WithContext(gateway.ContextWithValidatedPrincipalID(req.Context(), authzTestPrincipal))
	rec := httptest.NewRecorder()
	middleware.ServeHTTP(rec, req)

	t.Log("Then: Request is forbidden")
	assert.Equal(t, http.StatusForbidden, rec.Code)
	assert.Contains(t, rec.Body.String(), "Forbidden")
}

func TestAuthzMiddlewareNoPrincipal(t *testing.T) {
	t.Log("Given: Policy file granting all permissions")
	policy := setupPolicy(t, "testdata/policies/allow_all.yaml")
	defer policy.Close()

	t.Log("And: Middleware protecting a handler with read permission")
	middleware := gateway.AuthzMiddleware(policy, authzTestPermRead, testHandler)

	t.Log("When: Request without principal in context")
	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	rec := httptest.NewRecorder()
	middleware.ServeHTTP(rec, req)

	t.Log("Then: Request is forbidden")
	assert.Equal(t, http.StatusForbidden, rec.Code)
	assert.Contains(t, rec.Body.String(), "Forbidden")
}

func TestAuthzMiddlewareSelective(t *testing.T) {
	t.Log("Given: Policy file granting only read permission")
	policy := setupPolicy(t, "testdata/policies/selective.yaml")
	defer policy.Close()

	tests := []struct {
		name       string
		permission authz.PermissionName
		wantCode   int
		wantBody   string
	}{
		{
			name:       "allowed_read",
			permission: authzTestPermRead,
			wantCode:   http.StatusOK,
			wantBody:   "OK",
		},
		{
			name:       "denied_write",
			permission: authzTestPermWrite,
			wantCode:   http.StatusForbidden,
			wantBody:   "Forbidden",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Logf("When: Request requires %s permission", tc.permission)
			middleware := gateway.AuthzMiddleware(policy, tc.permission, testHandler)
			req := httptest.NewRequest(http.MethodGet, "/test", nil)
			req = req.WithContext(gateway.ContextWithValidatedPrincipalID(req.Context(), authzTestPrincipal))
			rec := httptest.NewRecorder()
			middleware.ServeHTTP(rec, req)

			t.Logf("Then: Request %s", tc.wantBody)
			assert.Equal(t, tc.wantCode, rec.Code)
			assert.Contains(t, rec.Body.String(), tc.wantBody)
		})
	}
}

func TestAuthzMiddlewareWrongPrincipal(t *testing.T) {
	t.Log("Given: Policy file granting permissions to specific principal")
	policy := setupPolicy(t, "testdata/policies/allow_all.yaml")
	defer policy.Close()

	t.Log("And: Middleware protecting a handler with read permission")
	middleware := gateway.AuthzMiddleware(policy, authzTestPermRead, testHandler)

	t.Log("When: Request with different principal not in policy")
	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	req = req.WithContext(gateway.ContextWithValidatedPrincipalID(req.Context(), authzOtherPrincipal))
	rec := httptest.NewRecorder()
	middleware.ServeHTTP(rec, req)

	t.Log("Then: Request is forbidden")
	assert.Equal(t, http.StatusForbidden, rec.Code)
	assert.Contains(t, rec.Body.String(), "Forbidden")
}

func TestAuthzMiddlewarePolicyReload(t *testing.T) {
	t.Log("Given: Policy file with allow_all")
	dir := t.TempDir()
	policyFile := filepath.Join(dir, "policy.yaml")

	copyPolicyFile := func(src string) {
		data, err := os.ReadFile(src)
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(policyFile, data, 0o644))
	}

	copyPolicyFile("testdata/policies/allow_all.yaml")
	policy := setupPolicy(t, policyFile)
	defer policy.Close()

	t.Log("And: Middleware protecting a handler with read permission")
	middleware := gateway.AuthzMiddleware(policy, authzTestPermRead, testHandler)

	t.Run("allow_all", func(t *testing.T) {
		t.Log("When: Request with valid principal")
		req := httptest.NewRequest(http.MethodGet, "/test", nil)
		req = req.WithContext(gateway.ContextWithValidatedPrincipalID(req.Context(), authzTestPrincipal))
		rec := httptest.NewRecorder()
		middleware.ServeHTTP(rec, req)

		t.Log("Then: Request succeeds")
		assert.Equal(t, http.StatusOK, rec.Code)
	})

	t.Log("Given: Policy file updated to deny_all")
	copyPolicyFile("testdata/policies/deny_all.yaml")
	time.Sleep(100 * time.Millisecond)

	t.Run("deny_all", func(t *testing.T) {
		t.Log("When: Request with valid principal")
		req := httptest.NewRequest(http.MethodGet, "/test", nil)
		req = req.WithContext(gateway.ContextWithValidatedPrincipalID(req.Context(), authzTestPrincipal))
		rec := httptest.NewRecorder()
		middleware.ServeHTTP(rec, req)

		t.Log("Then: Request fails")
		assert.Equal(t, http.StatusForbidden, rec.Code)
		assert.Contains(t, rec.Body.String(), "Forbidden")
	})
}

// setupPolicy creates a FileWatchingAuthzResourcePolicy for testing
func setupPolicy(t *testing.T, policyFile string) *gateway.FileWatchingAuthzResourcePolicy {
	t.Helper()
	policy, err := gateway.NewFileWatchingAuthzResourcePolicy(
		authzTestResourceName,
		policyFile,
		[]authz.PermissionName{authzTestPermRead, authzTestPermWrite},
		func(err error) {
			t.Fatalf("Policy watch error: %v", err)
		},
	)
	require.NoError(t, err)

	return policy
}
