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
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	policymaterializerv1connect "buf.build/gen/go/redpandadata/common/connectrpc/go/redpanda/policymaterializer/v1/policymaterializerv1connect"
	policymaterializerv1 "buf.build/gen/go/redpandadata/common/protocolbuffers/go/redpanda/policymaterializer/v1"
	"connectrpc.com/connect"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/common-go/authz"
	"github.com/redpanda-data/connect/v4/internal/gateway"
)

// fakePolicyMaterializerServer streams policies from a channel until it is closed.
type fakePolicyMaterializerServer struct {
	policies chan *policymaterializerv1.DataplanePolicy
}

func (f *fakePolicyMaterializerServer) WatchPolicy(
	ctx context.Context,
	_ *connect.Request[policymaterializerv1.WatchPolicyRequest],
	stream *connect.ServerStream[policymaterializerv1.WatchPolicyResponse],
) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		case p, ok := <-f.policies:
			if !ok {
				return nil
			}
			if err := stream.Send(&policymaterializerv1.WatchPolicyResponse{Policy: p}); err != nil {
				return err
			}
		}
	}
}

// startPolicyMaterializerServer starts an h2c Connect policy materializer server
// and returns its base URL.
func startPolicyMaterializerServer(t *testing.T, svc policymaterializerv1connect.PolicyMaterializerServiceHandler) string {
	t.Helper()
	mux := http.NewServeMux()
	path, handler := policymaterializerv1connect.NewPolicyMaterializerServiceHandler(svc)
	mux.Handle(path, handler)

	lis, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)

	srv := &http.Server{Handler: h2c.NewHandler(mux, &http2.Server{})}
	go srv.Serve(lis) //nolint:errcheck // test server
	t.Cleanup(func() { srv.Close() })

	return "http://" + lis.Addr().String()
}

// dataplanePolicy builds a DataplanePolicy granting permissions to a principal at a scope.
func dataplanePolicy(roleID string, permissions []string, principal, scope string) *policymaterializerv1.DataplanePolicy {
	perms := make([]string, len(permissions))
	copy(perms, permissions)
	return &policymaterializerv1.DataplanePolicy{
		Roles: []*policymaterializerv1.DataplaneRole{
			{Id: roleID, Permissions: perms},
		},
		Bindings: []*policymaterializerv1.DataplaneRoleBinding{
			{RoleId: roleID, Principal: principal, Scope: scope},
		},
	}
}

func TestEndpointWatchingAuthzPolicyAuthorizes(t *testing.T) {
	t.Log("Given: policy materializer endpoint serving an allow policy")
	policies := make(chan *policymaterializerv1.DataplanePolicy, 1)
	policies <- dataplanePolicy(
		"admin",
		[]string{string(authzTestPermRead), string(authzTestPermWrite)},
		string(authzTestPrincipal),
		string(authzTestResourceName),
	)
	addr := startPolicyMaterializerServer(t, &fakePolicyMaterializerServer{policies: policies})

	t.Log("And: policy loaded from endpoint")
	policy, err := gateway.NewEndpointWatchingAuthzResourcePolicy(
		authzTestResourceName,
		addr,
		[]authz.PermissionName{authzTestPermRead, authzTestPermWrite},
		func(err error) { t.Errorf("policy watch error: %v", err) },
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = policy.Close() })

	middleware := gateway.AuthzMiddleware(policy, authzTestPermRead, testHandler)

	t.Run("authorized_principal", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/test", http.NoBody)
		req = req.WithContext(gateway.ContextWithValidatedPrincipalID(req.Context(), authzTestPrincipal))
		rec := httptest.NewRecorder()
		middleware.ServeHTTP(rec, req)
		assert.Equal(t, http.StatusOK, rec.Code)
	})

	t.Run("unknown_principal", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/test", http.NoBody)
		req = req.WithContext(gateway.ContextWithValidatedPrincipalID(req.Context(), authzOtherPrincipal))
		rec := httptest.NewRecorder()
		middleware.ServeHTTP(rec, req)
		assert.Equal(t, http.StatusForbidden, rec.Code)
	})

	t.Run("no_principal", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/test", http.NoBody)
		rec := httptest.NewRecorder()
		middleware.ServeHTTP(rec, req)
		assert.Equal(t, http.StatusForbidden, rec.Code)
	})
}

func TestEndpointWatchingAuthzPolicyReload(t *testing.T) {
	t.Log("Given: policy materializer endpoint that will push two policies")
	policies := make(chan *policymaterializerv1.DataplanePolicy, 2)

	// Initial policy grants read to authzTestPrincipal.
	policies <- dataplanePolicy(
		"reader",
		[]string{string(authzTestPermRead)},
		string(authzTestPrincipal),
		string(authzTestResourceName),
	)

	addr := startPolicyMaterializerServer(t, &fakePolicyMaterializerServer{policies: policies})

	policy, err := gateway.NewEndpointWatchingAuthzResourcePolicy(
		authzTestResourceName,
		addr,
		[]authz.PermissionName{authzTestPermRead, authzTestPermWrite},
		func(err error) { t.Logf("policy watch callback: %v", err) },
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = policy.Close() })

	middleware := gateway.AuthzMiddleware(policy, authzTestPermRead, testHandler)

	t.Run("initial_policy_allows_read", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/test", http.NoBody)
		req = req.WithContext(gateway.ContextWithValidatedPrincipalID(req.Context(), authzTestPrincipal))
		rec := httptest.NewRecorder()
		middleware.ServeHTTP(rec, req)
		assert.Equal(t, http.StatusOK, rec.Code)
	})

	t.Log("When: endpoint pushes an updated policy granting no permissions")
	policies <- dataplanePolicy("empty", []string{}, string(authzTestPrincipal), string(authzTestResourceName))

	t.Run("updated_policy_denies_read", func(t *testing.T) {
		assert.Eventually(t, func() bool {
			req := httptest.NewRequest(http.MethodGet, "/test", http.NoBody)
			req = req.WithContext(gateway.ContextWithValidatedPrincipalID(req.Context(), authzTestPrincipal))
			rec := httptest.NewRecorder()
			middleware.ServeHTTP(rec, req)
			return rec.Code == http.StatusForbidden
		}, 5*time.Second, 50*time.Millisecond)
	})
}

func TestEndpointWatchingAuthzPolicyClose(t *testing.T) {
	policies := make(chan *policymaterializerv1.DataplanePolicy, 1)
	policies <- dataplanePolicy(
		"admin",
		[]string{string(authzTestPermRead)},
		string(authzTestPrincipal),
		string(authzTestResourceName),
	)
	addr := startPolicyMaterializerServer(t, &fakePolicyMaterializerServer{policies: policies})

	policy, err := gateway.NewEndpointWatchingAuthzResourcePolicy(
		authzTestResourceName,
		addr,
		[]authz.PermissionName{authzTestPermRead},
		func(err error) { t.Errorf("policy watch error: %v", err) },
	)
	require.NoError(t, err)
	assert.NoError(t, policy.Close())
}
