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

	"github.com/stretchr/testify/assert"
)

func TestOnceArgError(t *testing.T) {
	a := NewOnce(func(_ context.Context) error {
		t.Fatalf("Ack called")
		return nil
	})

	assert.NoError(t, a.Ack(t.Context(), errors.New("arg error")))
	assert.NoError(t, a.Ack(t.Context(), errors.New("arg error")))
	assert.EqualError(t, a.Wait(t.Context()), "arg error")
	assert.EqualError(t, a.Wait(t.Context()), "arg error")
}

func TestOnceAckError(t *testing.T) {
	a := NewOnce(func(_ context.Context) error {
		return errors.New("ack error")
	})

	assert.EqualError(t, a.Ack(t.Context(), nil), "ack error")
	assert.EqualError(t, a.Ack(t.Context(), nil), "ack error")
	assert.EqualError(t, a.Wait(t.Context()), "ack error")
	assert.EqualError(t, a.Wait(t.Context()), "ack error")
}

func TestOnceWaitContextCanceled(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	am := NewOnce(func(_ context.Context) error {
		return nil
	})

	assert.ErrorIs(t, am.Wait(ctx), context.Canceled)
}

func TestOnceAckOnce(t *testing.T) {
	ackCount := 0
	a := NewOnce(func(_ context.Context) error {
		ackCount++
		return nil
	})

	assert.NoError(t, a.Ack(t.Context(), nil))
	assert.NoError(t, a.Ack(t.Context(), nil))
	assert.NoError(t, a.Ack(t.Context(), nil))

	assert.Equal(t, 1, ackCount, "Ack should be called exactly once")
}
