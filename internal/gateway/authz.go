// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package gateway

import (
	"fmt"
	"sync/atomic"

	"github.com/redpanda-data/common-go/authz"
	"github.com/redpanda-data/common-go/authz/loader"
)

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
