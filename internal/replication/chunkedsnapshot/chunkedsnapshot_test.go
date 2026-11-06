// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package chunkedsnapshot

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRangesFromBounds(t *testing.T) {
	type key = []any
	tests := []struct {
		name   string
		bounds []key
		want   []Range[key]
	}{
		{name: "no bounds is one unbounded range", bounds: nil, want: []Range[key]{{}}},
		{name: "one bound", bounds: []key{{5}}, want: []Range[key]{{Max: key{5}}, {Min: key{5}}}},
		{name: "two bounds", bounds: []key{{5}, {9}}, want: []Range[key]{{Max: key{5}}, {Min: key{5}, Max: key{9}}, {Min: key{9}}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, RangesFromBounds(tt.bounds))
		})
	}
}

func TestBoundsFromSamples(t *testing.T) {
	samples := []int{1, 2, 3, 4, 5, 6, 7}
	tests := []struct {
		name string
		step int
		want []int
	}{
		{name: "every third", step: 3, want: []int{4, 7}},
		{name: "step larger than samples", step: 10, want: nil},
		{name: "zero step is treated as one", step: 0, want: []int{2, 3, 4, 5, 6, 7}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, BoundsFromSamples(samples, tt.step))
		})
	}
}

func TestRunStartsSingleTaskTablesFirst(t *testing.T) {
	var (
		mu      sync.Mutex
		started []string
	)
	task := func(name string) Task {
		return func(context.Context) error {
			mu.Lock()
			defer mu.Unlock()
			started = append(started, name)
			return nil
		}
	}

	err := Run(t.Context(), 1, [][]Task{
		{task("chunked-1"), task("chunked-2")},
		{task("serial")},
	})

	require.NoError(t, err)
	assert.Equal(t, []string{"serial", "chunked-1", "chunked-2"}, started)
}

func TestRunReturnsFirstError(t *testing.T) {
	errBoom := errors.New("boom")

	err := Run(t.Context(), 2, [][]Task{
		{func(context.Context) error { return nil }},
		{func(context.Context) error { return errBoom }},
	})

	assert.ErrorIs(t, err, errBoom)
}

func TestCompletionCallsOnCompleteOnceAfterLastTask(t *testing.T) {
	var calls []int64
	c := NewCompletion(3, func(rows int64) { calls = append(calls, rows) })

	c.Done(1)
	c.Done(2)
	assert.Empty(t, calls, "Expected no completion before the last task")

	c.Done(3)
	assert.Equal(t, []int64{6}, calls)
}
