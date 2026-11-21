// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package plan

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// StackLockKey is the object that serialises use of the shared bench stack.
const StackLockKey = "locks/shared-stack"

// DefaultLockTTL covers a 4-point e2e sweep plus infra and matches the reaper
// TTL, so a crashed holder frees the stack no later than the reaper would.
const DefaultLockTTL = 4 * time.Hour

// Lock is the shared-stack lock. Only one holder at a time may run on the
// stack; secret isolation depends on that.
type Lock interface {
	// Acquire takes the lock for ttl. It returns false, with no error, when
	// another holder has an unexpired lock or wins a race for it.
	Acquire(ctx context.Context, holder string, ttl time.Duration) (bool, error)
	// Release deletes the lock only when holder is the current holder;
	// otherwise it is a no-op.
	Release(ctx context.Context, holder string) error
}

type lockBody struct {
	Holder    string    `json:"holder"`
	ExpiresAt time.Time `json:"expires_at"`
}

type objectLock struct {
	objs Objects
	now  func() time.Time
}

// NewLock returns a Lock over any Objects.
func NewLock(objs Objects) Lock { return &objectLock{objs: objs, now: time.Now} }

// NewS3Lock returns a Lock kept in bucket.
func NewS3Lock(api S3API, bucket string) Lock { return NewLock(NewS3Objects(api, bucket)) }

// SetClock replaces the lock's clock; for tests.
func (l *objectLock) SetClock(now func() time.Time) { l.now = now }

func (l *objectLock) Acquire(ctx context.Context, holder string, ttl time.Duration) (bool, error) {
	if holder == "" {
		return false, errors.New("lock holder must not be empty")
	}
	if ttl <= 0 {
		return false, errors.New("lock ttl must be positive")
	}
	body, err := json.Marshal(lockBody{Holder: holder, ExpiresAt: l.now().Add(ttl).UTC()})
	if err != nil {
		return false, err
	}
	err = l.objs.Create(ctx, StackLockKey, body)
	switch {
	case err == nil:
		return true, nil
	case !errors.Is(err, ErrConflict):
		return false, fmt.Errorf("acquiring stack lock: %w", err)
	}

	cur, etag, err := l.objs.Get(ctx, StackLockKey)
	if errors.Is(err, ErrNotFound) {
		return false, nil // released between our two calls; let the caller poll again
	}
	if err != nil {
		return false, fmt.Errorf("reading stack lock: %w", err)
	}
	var held lockBody
	if err := json.Unmarshal(cur, &held); err != nil {
		return false, fmt.Errorf("parsing stack lock: %w", err)
	}
	if l.now().Before(held.ExpiresAt) {
		return false, nil
	}
	// Expired: replace against the ETag we read, so only one contender wins.
	switch err := l.objs.Replace(ctx, StackLockKey, body, etag); {
	case err == nil:
		return true, nil
	case errors.Is(err, ErrConflict):
		return false, nil
	default:
		return false, fmt.Errorf("taking over expired stack lock: %w", err)
	}
}

func (l *objectLock) Release(ctx context.Context, holder string) error {
	cur, etag, err := l.objs.Get(ctx, StackLockKey)
	if errors.Is(err, ErrNotFound) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("reading stack lock: %w", err)
	}
	var held lockBody
	if err := json.Unmarshal(cur, &held); err != nil || held.Holder != holder {
		return nil
	}
	if err := l.objs.Delete(ctx, StackLockKey, etag); err != nil && !errors.Is(err, ErrConflict) {
		return fmt.Errorf("releasing stack lock: %w", err)
	}
	return nil // a conflict means someone took it over after expiry; not ours any more
}
