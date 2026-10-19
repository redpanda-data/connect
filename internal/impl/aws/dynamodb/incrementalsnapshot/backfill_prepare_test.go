// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package incrementalsnapshot

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"
)

type fakeProgress struct{ complete, segments bool }

func (p fakeProgress) IsComplete() bool         { return p.complete }
func (p fakeProgress) HasSegmentProgress() bool { return p.segments }

// prepareDeps is a Deps for Prepare: it records the store calls in order.
// Scan and NewEmitter are never called by Prepare.
type prepareDeps struct {
	progress fakeProgress
	stale    bool
	staleErr error
	calls    []string
}

func (d *prepareDeps) SnapshotProgress(context.Context) (fakeProgress, error) {
	d.calls = append(d.calls, "progress")
	return d.progress, nil
}

func (d *prepareDeps) MarkSnapshotComplete(context.Context) error {
	d.calls = append(d.calls, "complete")
	return nil
}

func (d *prepareDeps) ResetSnapshotProgress(context.Context) error {
	d.calls = append(d.calls, "reset")
	return nil
}

func (d *prepareDeps) MarkSnapshotRequested(context.Context) error {
	d.calls = append(d.calls, "requested")
	return nil
}

func (*prepareDeps) SnapshotRequested(context.Context) (bool, error) { return false, nil }

func (d *prepareDeps) CDCCheckpointStale(context.Context) (bool, error) {
	d.calls = append(d.calls, "stale?")
	return d.stale, d.staleErr
}

func (*prepareDeps) NewEmitter() Emitter { panic("Prepare must not emit") }

func (*prepareDeps) Scan(context.Context, fakeProgress, ScanHooks) error {
	panic("Prepare must not scan")
}

func prepareBackfill(deps *prepareDeps) *Backfill[fakeProgress] {
	return &Backfill[fakeProgress]{Table: "t", Deps: deps, Logger: service.MockResources().Logger()}
}

func TestPrepareInProgressSnapshot(t *testing.T) {
	tests := []struct {
		name       string
		stale      bool
		staleErr   error
		signalOnly bool
		wantReset  bool
		wantCalls  []string
	}{
		{
			name:      "stale checkpoint resets the in-progress snapshot",
			stale:     true,
			wantReset: true,
			wantCalls: []string{"progress", "stale?", "reset"},
		},
		{
			name:       "stale checkpoint in signal-only mode records the request before the reset",
			stale:      true,
			signalOnly: true,
			wantReset:  true,
			wantCalls:  []string{"progress", "stale?", "requested", "reset"},
		},
		{
			name:      "fresh checkpoint resumes without a reset",
			wantCalls: []string{"progress", "stale?"},
		},
		{
			name:      "failed staleness check resumes without a reset",
			staleErr:  errors.New("describe failed"),
			wantCalls: []string{"progress", "stale?"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			deps := &prepareDeps{progress: fakeProgress{segments: true}, stale: tt.stale, staleErr: tt.staleErr}
			skip, reset, err := prepareBackfill(deps).Prepare(t.Context(), tt.signalOnly)
			require.NoError(t, err)
			assert.False(t, skip, "an in-progress snapshot is never skipped")
			assert.Equal(t, tt.wantReset, reset)
			assert.Equal(t, tt.wantCalls, deps.calls)
		})
	}
}

func TestPrepareFreshTableSkipsStalenessCheck(t *testing.T) {
	deps := &prepareDeps{stale: true}
	skip, reset, err := prepareBackfill(deps).Prepare(t.Context(), false)
	require.NoError(t, err)
	assert.False(t, skip)
	assert.False(t, reset)
	assert.Equal(t, []string{"progress"}, deps.calls, "a table with no snapshot rows has nothing to reset")
}

func TestPrepareCompletedSnapshot(t *testing.T) {
	tests := []struct {
		name      string
		stale     bool
		staleErr  error
		wantSkip  bool
		wantReset bool
	}{
		{name: "valid snapshot is skipped", wantSkip: true},
		{name: "stale snapshot is reset", stale: true, wantReset: true},
		{name: "failed staleness check keeps the snapshot", staleErr: errors.New("describe failed"), wantSkip: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			deps := &prepareDeps{progress: fakeProgress{complete: true, segments: true}, stale: tt.stale, staleErr: tt.staleErr}
			skip, reset, err := prepareBackfill(deps).Prepare(t.Context(), false)
			require.NoError(t, err)
			assert.Equal(t, tt.wantSkip, skip)
			assert.Equal(t, tt.wantReset, reset)
		})
	}
}

// runDeps is a Deps whose Scan fails and whose emitter's WaitAcked blocks
// until release is closed or ctx ends.
type runDeps struct {
	prepareDeps
	waited  chan struct{}
	release chan struct{}
}

func (d *runDeps) NewEmitter() Emitter { return &gateEmitter{d: d} }

func (*runDeps) Scan(context.Context, fakeProgress, ScanHooks) error {
	return errors.New("stubbed scan failure")
}

type gateEmitter struct{ d *runDeps }

func (*gateEmitter) Emit(context.Context, []Item, int, Item) error { return nil }
func (*gateEmitter) SealSegment(context.Context, int) error        { return nil }
func (*gateEmitter) FlushCompleted(context.Context) error          { return nil }

func (e *gateEmitter) WaitAcked(ctx context.Context) error {
	select {
	case e.d.waited <- struct{}{}:
	default:
	}
	select {
	case <-e.d.release:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func newRunBackfill(deps *runDeps) *Backfill[fakeProgress] {
	return &Backfill[fakeProgress]{
		Table:  "t",
		State:  NewState(0, time.Minute),
		Deps:   deps,
		Logger: service.MockResources().Logger(),
	}
}

// TestRunFailureWaitsForAcks: a failed run returns only once its emitted
// pages have settled, so a retry (and its stale reset) never overlaps the
// failed run's late acks.
func TestRunFailureWaitsForAcks(t *testing.T) {
	deps := &runDeps{waited: make(chan struct{}, 1), release: make(chan struct{})}
	errCh := make(chan error, 1)
	go func() { errCh <- newRunBackfill(deps).Run(t.Context()) }()

	select {
	case <-deps.waited:
	case <-time.After(5 * time.Second):
		t.Fatal("the failed run did not wait for its acks")
	}
	select {
	case err := <-errCh:
		t.Fatalf("the failed run returned before its acks settled: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	close(deps.release)
	select {
	case err := <-errCh:
		require.ErrorContains(t, err, "stubbed scan failure")
	case <-time.After(5 * time.Second):
		t.Fatal("the failed run did not return once its acks settled")
	}
}

// TestRunFailureOnShutdownReturnsAtOnce: a run that fails because ctx ended
// does not wait for acks, so shutdown stays prompt.
func TestRunFailureOnShutdownReturnsAtOnce(t *testing.T) {
	deps := &runDeps{waited: make(chan struct{}, 1), release: make(chan struct{})}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	errCh := make(chan error, 1)
	go func() { errCh <- newRunBackfill(deps).Run(ctx) }()
	select {
	case err := <-errCh:
		require.Error(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("a cancelled run did not return at once")
	}
	select {
	case <-deps.waited:
		t.Fatal("a cancelled run must not wait for acks")
	default:
	}
}
