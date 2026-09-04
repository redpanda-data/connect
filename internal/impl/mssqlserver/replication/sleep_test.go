// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package replication

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestSleepInterruptible(t *testing.T) {
	t.Run("cancellation cuts the sleep short", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		go func() {
			time.Sleep(10 * time.Millisecond)
			cancel()
		}()
		start := time.Now()
		err := sleepInterruptible(ctx, time.Hour)
		require.ErrorIs(t, err, context.Canceled)
		require.Less(t, time.Since(start), 10*time.Second)
	})

	t.Run("uncancelled sleep elapses and returns nil", func(t *testing.T) {
		require.NoError(t, sleepInterruptible(t.Context(), time.Millisecond))
	})
}
