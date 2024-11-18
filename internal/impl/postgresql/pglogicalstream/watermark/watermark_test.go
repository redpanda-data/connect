/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

package watermark_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/connect/v4/internal/impl/postgresql/pglogicalstream/watermark"
)

func TestWatermark(t *testing.T) {
	w := watermark.New(5)
	require.Equal(t, 5, w.Get())
	w.Set(3)
	require.Equal(t, 5, w.Get())
	require.Len(t, w.WaitFor(1), 1)
	ch1 := w.WaitFor(9)
	ch2 := w.WaitFor(10)
	ch3 := w.WaitFor(10)
	ch4 := w.WaitFor(100)
	require.Empty(t, ch1)
	require.Empty(t, ch2)
	require.Empty(t, ch3)
	require.Empty(t, ch4)
	w.Set(8)
	require.Equal(t, 8, w.Get())
	require.Empty(t, ch1)
	require.Empty(t, ch2)
	require.Empty(t, ch3)
	require.Empty(t, ch4)
	w.Set(9)
	require.Equal(t, 9, w.Get())
	require.Len(t, ch1, 1)
	require.Empty(t, ch2)
	require.Empty(t, ch3)
	require.Empty(t, ch4)
	w.Set(10)
	require.Equal(t, 10, w.Get())
	require.Len(t, ch1, 1)
	require.Len(t, ch2, 1)
	require.Len(t, ch3, 1)
	require.Empty(t, ch4)
}
