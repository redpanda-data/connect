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
	"errors"
	"testing"
)

func TestOnceArgError(t *testing.T) {
	a := NewOnce(func(ctx context.Context) error {
		t.Fatalf("Ack called")
		return nil
	})

	if err := a.Ack(t.Context(), errors.New("arg error")); !errEqual(err, nil) {
		t.Fatalf("Ack() error = %v, want %v", err, errors.New("arg error"))
	}
	if err := a.Wait(t.Context()); !errEqual(err, errors.New("arg error")) {
		t.Fatalf("Wait() error = %v, want %v", err, errors.New("arg error"))
	}
}

func TestOnceAckError(t *testing.T) {
	a := NewOnce(func(ctx context.Context) error {
		return errors.New("ack error")
	})

	if err := a.Ack(t.Context(), nil); !errEqual(err, errors.New("ack error")) {
		t.Fatalf("Ack() error = %v, want %v", err, errors.New("ack error"))
	}
	if err := a.Wait(t.Context()); !errEqual(err, errors.New("ack error")) {
		t.Fatalf("Wait() error = %v, want %v", err, errors.New("ack error"))
	}
}

func TestOnceWaitContextCanceled(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	am := NewOnce(func(ctx context.Context) error {
		return nil
	})

	if err := am.Wait(ctx); !errors.Is(err, context.Canceled) {
		t.Errorf("Wait() error = %v, want %v", err, context.Canceled)
	}
}

func TestOnceAckOnce(t *testing.T) {
	ackCount := 0
	ack := func(ctx context.Context) error {
		ackCount++
		return nil
	}

	a := NewOnce(ack)
	_ = a.Ack(t.Context(), nil)
	_ = a.Ack(t.Context(), nil)
	_ = a.Ack(t.Context(), nil)

	if ackCount != 1 {
		t.Errorf("Ack called %d times, want 1", ackCount)
	}
}

func errEqual(a, b error) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	return a.Error() == b.Error()
}
