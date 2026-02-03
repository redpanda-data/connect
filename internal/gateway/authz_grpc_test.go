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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/redpanda-data/common-go/authz"
	"github.com/redpanda-data/connect/v4/internal/gateway"
)

// testUnaryHandler is a simple gRPC unary handler for testing
func testUnaryHandler(_ context.Context, _ any) (any, error) {
	return "OK", nil
}

// testStreamHandler is a simple gRPC stream handler for testing
func testStreamHandler(_ any, _ grpc.ServerStream) error {
	return nil
}

// mockServerStream implements grpc.ServerStream for testing
type mockServerStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (m *mockServerStream) Context() context.Context {
	return m.ctx
}

func TestGRPCUnaryAuthzInterceptorAllowAll(t *testing.T) {
	t.Log("Given: Policy file granting all permissions")
	policy := setupPolicy(t, "testdata/policies/allow_all.yaml")
	defer policy.Close()

	t.Log("And: Unary interceptor with read permission")
	interceptor := gateway.GRPCUnaryAuthzInterceptor(policy, authzTestPermRead)

	t.Log("When: Request with valid principal in context")
	ctx := gateway.ContextWithValidatedPrincipalID(context.Background(), authzTestPrincipal)
	result, err := interceptor(ctx, nil, &grpc.UnaryServerInfo{}, testUnaryHandler)

	t.Log("Then: Request succeeds")
	require.NoError(t, err)
	assert.Equal(t, "OK", result)
}

func TestGRPCUnaryAuthzInterceptorDenyAll(t *testing.T) {
	t.Log("Given: Policy file denying all permissions")
	policy := setupPolicy(t, "testdata/policies/deny_all.yaml")
	defer policy.Close()

	t.Log("And: Unary interceptor with read permission")
	interceptor := gateway.GRPCUnaryAuthzInterceptor(policy, authzTestPermRead)

	t.Log("When: Request with valid principal but no permissions")
	ctx := gateway.ContextWithValidatedPrincipalID(context.Background(), authzTestPrincipal)
	_, err := interceptor(ctx, nil, &grpc.UnaryServerInfo{}, testUnaryHandler)

	t.Log("Then: Request fails with PermissionDenied")
	require.Error(t, err)
	assert.Equal(t, codes.PermissionDenied, status.Code(err))
}

func TestGRPCUnaryAuthzInterceptorNoPrincipal(t *testing.T) {
	t.Log("Given: Policy file granting all permissions")
	policy := setupPolicy(t, "testdata/policies/allow_all.yaml")
	defer policy.Close()

	t.Log("And: Unary interceptor with read permission")
	interceptor := gateway.GRPCUnaryAuthzInterceptor(policy, authzTestPermRead)

	t.Log("When: Request without principal in context")
	_, err := interceptor(context.Background(), nil, &grpc.UnaryServerInfo{}, testUnaryHandler)

	t.Log("Then: Request fails with PermissionDenied")
	require.Error(t, err)
	assert.Equal(t, codes.PermissionDenied, status.Code(err))
}

func TestGRPCUnaryAuthzInterceptorSelective(t *testing.T) {
	t.Log("Given: Policy file granting only read permission")
	policy := setupPolicy(t, "testdata/policies/selective.yaml")
	defer policy.Close()

	tests := []struct {
		name     string
		perm     string
		wantErr  bool
		wantCode codes.Code
	}{
		{
			name:    "allowed_read",
			perm:    string(authzTestPermRead),
			wantErr: false,
		},
		{
			name:     "denied_write",
			perm:     string(authzTestPermWrite),
			wantErr:  true,
			wantCode: codes.PermissionDenied,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Logf("When: Request requires %s permission", tc.perm)
			interceptor := gateway.GRPCUnaryAuthzInterceptor(policy, authz.PermissionName(tc.perm))
			ctx := gateway.ContextWithValidatedPrincipalID(context.Background(), authzTestPrincipal)
			_, err := interceptor(ctx, nil, &grpc.UnaryServerInfo{}, testUnaryHandler)

			if tc.wantErr {
				t.Log("Then: Request fails with PermissionDenied")
				require.Error(t, err)
				assert.Equal(t, tc.wantCode, status.Code(err))
			} else {
				t.Log("Then: Request succeeds")
				require.NoError(t, err)
			}
		})
	}
}

func TestGRPCUnaryAuthzInterceptorWrongPrincipal(t *testing.T) {
	t.Log("Given: Policy file granting permissions to specific principal")
	policy := setupPolicy(t, "testdata/policies/allow_all.yaml")
	defer policy.Close()

	t.Log("And: Unary interceptor with read permission")
	interceptor := gateway.GRPCUnaryAuthzInterceptor(policy, authzTestPermRead)

	t.Log("When: Request with different principal not in policy")
	ctx := gateway.ContextWithValidatedPrincipalID(context.Background(), authzOtherPrincipal)
	_, err := interceptor(ctx, nil, &grpc.UnaryServerInfo{}, testUnaryHandler)

	t.Log("Then: Request fails with PermissionDenied")
	require.Error(t, err)
	assert.Equal(t, codes.PermissionDenied, status.Code(err))
}

func TestGRPCStreamAuthzInterceptorAllowAll(t *testing.T) {
	t.Log("Given: Policy file granting all permissions")
	policy := setupPolicy(t, "testdata/policies/allow_all.yaml")
	defer policy.Close()

	t.Log("And: Stream interceptor with read permission")
	interceptor := gateway.GRPCStreamAuthzInterceptor(policy, authzTestPermRead)

	t.Log("When: Stream request with valid principal in context")
	ctx := gateway.ContextWithValidatedPrincipalID(context.Background(), authzTestPrincipal)
	ss := &mockServerStream{ctx: ctx}
	err := interceptor(nil, ss, &grpc.StreamServerInfo{}, testStreamHandler)

	t.Log("Then: Request succeeds")
	require.NoError(t, err)
}

func TestGRPCStreamAuthzInterceptorDenyAll(t *testing.T) {
	t.Log("Given: Policy file denying all permissions")
	policy := setupPolicy(t, "testdata/policies/deny_all.yaml")
	defer policy.Close()

	t.Log("And: Stream interceptor with read permission")
	interceptor := gateway.GRPCStreamAuthzInterceptor(policy, authzTestPermRead)

	t.Log("When: Stream request with valid principal but no permissions")
	ctx := gateway.ContextWithValidatedPrincipalID(context.Background(), authzTestPrincipal)
	ss := &mockServerStream{ctx: ctx}
	err := interceptor(nil, ss, &grpc.StreamServerInfo{}, testStreamHandler)

	t.Log("Then: Request fails with PermissionDenied")
	require.Error(t, err)
	assert.Equal(t, codes.PermissionDenied, status.Code(err))
}

func TestGRPCStreamAuthzInterceptorNoPrincipal(t *testing.T) {
	t.Log("Given: Policy file granting all permissions")
	policy := setupPolicy(t, "testdata/policies/allow_all.yaml")
	defer policy.Close()

	t.Log("And: Stream interceptor with read permission")
	interceptor := gateway.GRPCStreamAuthzInterceptor(policy, authzTestPermRead)

	t.Log("When: Stream request without principal in context")
	ss := &mockServerStream{ctx: context.Background()}
	err := interceptor(nil, ss, &grpc.StreamServerInfo{}, testStreamHandler)

	t.Log("Then: Request fails with PermissionDenied")
	require.Error(t, err)
	assert.Equal(t, codes.PermissionDenied, status.Code(err))
}
