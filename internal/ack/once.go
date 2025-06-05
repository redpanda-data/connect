// Copyright 2025 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package ack

import (
	"context"
	"sync"
)

// Once wraps an ack function and ensures that it is called at most once. Ack
// will return the same result every time. Wait can be called once. If Ack is
// called with error the ack is not called and error is propagated to Wait.
// Otherwise, Ack returns ack result and the result is also propagated to Wait.
type Once struct {
	ack     func(ctx context.Context) error
	once    sync.Once
	ackErr  error
	waitErr error
	done    chan struct{}
}

// NewOnce creates new Once.
func NewOnce(ack func(ctx context.Context) error) *Once {
	return &Once{
		ack:    ack,
		done:   make(chan struct{}),
		once:   sync.Once{},
		ackErr: nil,
	}
}

// Ack is service.AckFunc that ensures that ack is called at most once.
// See Once for details.
func (a *Once) Ack(ctx context.Context, err error) error {
	a.once.Do(func() {
		if err != nil {
			a.waitErr = err
		} else {
			a.ackErr = a.ack(ctx)
			a.waitErr = a.ackErr
		}
		close(a.done)
	})

	return a.ackErr
}

// Wait waits for Ack call and returns the Ack error. See Once for details.
// Wait can be called multiple times and will always return the same result
// if Ack was called.
func (a *Once) Wait(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-a.done:
		return a.waitErr
	}
}

// TryWait returns true if Ack was called and false otherwise. If Ack was called
// the Ack error is returned.
func (a *Once) TryWait() (bool, error) {
	select {
	case <-a.done:
		return true, a.waitErr
	default:
		return false, nil
	}
}
