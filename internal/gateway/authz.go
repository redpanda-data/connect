// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package gateway

import (
	"context"
	"fmt"
	"net/http"
	"sync/atomic"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/common-go/authz"
	"github.com/redpanda-data/common-go/authz/loader"
)

// AuthzConfig holds the configuration for authorization policy.
type AuthzConfig struct {
	ResourceName authz.ResourceName
	PolicyFile   string
}

type authzConfigKeyType int

var authzConfigKey authzConfigKeyType

// SetManagerAuthzConfig stores the authorization configuration in the resource
// manager.
func SetManagerAuthzConfig(mgr *service.Resources, conf AuthzConfig) {
	mgr.SetGeneric(authzConfigKey, conf)
}

// ManagerAuthzConfig retrieves the authorization configuration from the
// resource manager.
func ManagerAuthzConfig(mgr *service.Resources) (AuthzConfig, bool) {
	if c, ok := mgr.GetGeneric(authzConfigKey); ok {
		return c.(AuthzConfig), true
	}
	return AuthzConfig{}, false
}

// FileWatchingAuthzResourcePolicy wraps an authorization policy that
// automatically reloads when the underlying policy file changes.
// Thread-safe for concurrent use.
type FileWatchingAuthzResourcePolicy struct {
	unwatch loader.PolicyUnwatch
	value   atomic.Pointer[authz.ResourcePolicy]
}

// NewFileWatchingAuthzResourcePolicy loads an authorization policy from file and
// watches it for changes. The notifyError callback is called on reload errors.
func NewFileWatchingAuthzResourcePolicy(
	name authz.ResourceName,
	file string,
	permissions []authz.PermissionName,
	notifyError func(error),
) (*FileWatchingAuthzResourcePolicy, error) {
	a := new(FileWatchingAuthzResourcePolicy)

	policy, unwatch, err := loader.WatchPolicyFile(file, func(policy authz.Policy, err error) {
		if err != nil {
			notifyError(fmt.Errorf("watching authorization policy file: %w", err))
			return
		}
		rp, err := authz.NewResourcePolicy(policy, name, permissions)
		if err != nil {
			notifyError(fmt.Errorf("loading authorization policy: %w", err))
			return
		}
		a.value.Store(rp)
	})
	if err != nil {
		return nil, fmt.Errorf("load authorization policy file: %w", err)
	}
	a.unwatch = unwatch

	rp, err := authz.NewResourcePolicy(policy, name, permissions)
	if err != nil {
		return nil, fmt.Errorf("compile authorization policy: %w", err)
	}
	a.value.Store(rp)

	return a, nil
}

// Close closes the resource policy and stops watching the policy file.
func (r *FileWatchingAuthzResourcePolicy) Close() error {
	if r == nil {
		return nil
	}
	return r.unwatch()
}

// Authorizer returns an [Authorizer] for this resource and the given permission.
// The permission must have been provided to [NewFileWatchingAuthzResourcePolicy].
func (r *FileWatchingAuthzResourcePolicy) Authorizer(perm authz.PermissionName) authz.Authorizer {
	return r.value.Load().Authorizer(perm)
}

// SubResourceAuthorizer returns an [Authorizer] for a child resource and
// the given permission. The permission must have been provided to
// [NewFileWatchingAuthzResourcePolicy].
func (r *FileWatchingAuthzResourcePolicy) SubResourceAuthorizer(t authz.ResourceType, id authz.ResourceID, perm authz.PermissionName) authz.Authorizer {
	return r.value.Load().SubResourceAuthorizer(t, id, perm)
}

// AuthzMiddleware returns an HTTP middleware handler that enforces
// authorization checks for the given permission before invoking the next
// handler. If the principal is missing or unauthorized, it responds with
// 403 Forbidden.
func AuthzMiddleware(
	policy *FileWatchingAuthzResourcePolicy,
	perm authz.PermissionName,
	next http.Handler,
) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		principal, ok := ValidatedPrincipalIDFromContext(req.Context())
		if !ok || !policy.Authorizer(perm).Check(principal) {
			http.Error(w, "Forbidden", http.StatusForbidden)
			return
		}
		next.ServeHTTP(w, req)
	})
}

// GRPCUnaryAuthzInterceptor returns a gRPC unary interceptor that enforces
// authorization checks for the given permission before invoking the handler.
// If the principal is missing or unauthorized, it returns PermissionDenied.
func GRPCUnaryAuthzInterceptor(
	policy *FileWatchingAuthzResourcePolicy,
	perm authz.PermissionName,
) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		principal, ok := ValidatedPrincipalIDFromContext(ctx)
		if !ok || !policy.Authorizer(perm).Check(principal) {
			return nil, status.Error(codes.PermissionDenied, "permission denied")
		}
		return handler(ctx, req)
	}
}

// GRPCStreamAuthzInterceptor returns a gRPC stream interceptor that enforces
// authorization checks for the given permission before invoking the handler.
// If the principal is missing or unauthorized, it returns PermissionDenied.
func GRPCStreamAuthzInterceptor(
	policy *FileWatchingAuthzResourcePolicy,
	perm authz.PermissionName,
) grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, _ *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		principal, ok := ValidatedPrincipalIDFromContext(ss.Context())
		if !ok || !policy.Authorizer(perm).Check(principal) {
			return status.Error(codes.PermissionDenied, "permission denied")
		}
		return handler(srv, ss)
	}
}
