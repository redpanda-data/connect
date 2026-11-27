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
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

var t0 = time.Date(2026, 11, 20, 12, 0, 0, 0, time.UTC)

// clockedStores is stores(t) with a settable clock shared by both stores.
func clockedStores(t *testing.T, now *time.Time) map[string]Store {
	clock := WithClock(func() time.Time { return *now })
	return map[string]Store{
		"s3":  NewStore(NewS3Objects(newFakeS3(), "bucket"), clock),
		"dir": NewStore(NewDirObjects(t.TempDir()), clock),
	}
}

func TestObjectsList(t *testing.T) {
	for name, objs := range map[string]Objects{
		"s3":  NewS3Objects(newFakeS3(), "bucket"),
		"dir": NewDirObjects(t.TempDir()),
	} {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			for _, k := range []string{"plans/b.json", "plans/a.json", "plans/a.done", "locks/shared-stack", "plans/c.run"} {
				require.NoError(t, objs.Create(ctx, k, []byte("{}")))
			}
			got, err := objs.List(ctx, "plans/")
			require.NoError(t, err)
			require.Equal(t, []string{"plans/a.done", "plans/a.json", "plans/b.json", "plans/c.run"}, got, "sorted, every page, prefix only")
			got, err = objs.List(ctx, "nothing/")
			require.NoError(t, err)
			require.Empty(t, got)
		})
	}
}

func TestGetReportsDone(t *testing.T) {
	for name, s := range stores(t) {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			p := testPlan("0123456789ab")
			require.NoError(t, s.Put(ctx, p))
			got, err := s.Get(ctx, p.ID)
			require.NoError(t, err)
			require.False(t, got.Done)
			require.Empty(t, got.Outcome)

			require.NoError(t, s.MarkDone(ctx, p.ID, OutcomeSuccess))
			got, err = s.Get(ctx, p.ID)
			require.NoError(t, err)
			require.True(t, got.Done)
			require.Equal(t, OutcomeSuccess, got.Outcome)
		})
	}
}

func TestPutIgnoresDoneFields(t *testing.T) {
	for name, s := range stores(t) {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			p := testPlan("0123456789ab")
			p.Done, p.Outcome = true, OutcomeSuccess
			require.NoError(t, s.Put(ctx, p))
			got, err := s.Get(ctx, p.ID)
			require.NoError(t, err)
			require.False(t, got.Done, "only the done marker says a plan is done")
			require.Empty(t, got.Outcome)
		})
	}
}

func TestStale(t *testing.T) {
	const (
		unclaimed     = "000000000001"
		fresh         = "000000000002" // claimed 10m ago, no run yet
		neverStarted  = "000000000003" // claimed 40m ago, no run
		running       = "000000000004" // claimed 2h ago, run recorded
		neverReported = "000000000005" // claimed 7h ago, run recorded, no done
		done          = "000000000006" // claimed 7h ago, run recorded, done
		oldNoRun      = "000000000007" // claimed 7h ago, never started
	)
	now := t0
	for name, s := range clockedStores(t, &now) {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			claim := func(id string, ago time.Duration, runID int64, isDone bool) {
				t.Helper()
				require.NoError(t, s.Put(ctx, testPlan(id)))
				now = t0.Add(-ago)
				first, err := s.Claim(ctx, id, "alice")
				require.NoError(t, err)
				require.True(t, first)
				if runID > 0 {
					require.NoError(t, s.RecordRun(ctx, id, runID))
				}
				if isDone {
					require.NoError(t, s.MarkDone(ctx, id, OutcomeSuccess))
				}
				now = t0
			}
			require.NoError(t, s.Put(ctx, testPlan(unclaimed)))
			claim(fresh, 10*time.Minute, 0, false)
			claim(neverStarted, 40*time.Minute, 0, false)
			claim(running, 2*time.Hour, 4, false)
			claim(neverReported, 7*time.Hour, 5, false)
			claim(done, 7*time.Hour, 6, true)
			claim(oldNoRun, 7*time.Hour, 0, false)

			got, err := s.Stale(ctx, t0, 6*time.Hour, 30*time.Minute)
			require.NoError(t, err)
			require.Equal(t, []StalePlan{
				{ID: neverStarted, Outcome: OutcomeNeverStarted, ClaimedAt: t0.Add(-40 * time.Minute)},
				{ID: neverReported, Outcome: OutcomeNeverReported, RunID: 5, ClaimedAt: t0.Add(-7 * time.Hour)},
				{ID: oldNoRun, Outcome: OutcomeNeverStarted, ClaimedAt: t0.Add(-7 * time.Hour)},
			}, got)

			// Once reported (marked done), a plan is never reported again.
			require.NoError(t, s.MarkDone(ctx, neverStarted, OutcomeNeverStarted))
			got, err = s.Stale(ctx, t0, 6*time.Hour, 30*time.Minute)
			require.NoError(t, err)
			require.Len(t, got, 2)
			require.Equal(t, neverReported, got[0].ID)
		})
	}
}

func TestStaleBoundaries(t *testing.T) {
	now := t0
	s := NewStore(NewDirObjects(t.TempDir()), WithClock(func() time.Time { return now }))
	ctx := context.Background()
	require.NoError(t, s.Put(ctx, testPlan("0123456789ab")))
	now = t0.Add(-30 * time.Minute)
	_, err := s.Claim(ctx, "0123456789ab", "a")
	require.NoError(t, err)
	got, err := s.Stale(ctx, t0, 6*time.Hour, 30*time.Minute)
	require.NoError(t, err)
	require.Empty(t, got, "exactly at the threshold is not yet stale")
	got, err = s.Stale(ctx, t0.Add(time.Second), 6*time.Hour, 30*time.Minute)
	require.NoError(t, err)
	require.Len(t, got, 1)
}

func TestStaleSkipsMalformedKeys(t *testing.T) {
	objs := NewDirObjects(t.TempDir())
	ctx := context.Background()
	// A marker whose name is not a plan id must never become a plan id.
	require.NoError(t, objs.Create(ctx, "plans/NOT-AN-ID.dispatched", []byte(`{"approver":"x","at":"2020-01-01T00:00:00Z"}`)))
	require.NoError(t, objs.Create(ctx, "plans/0123456789ab.dispatched.bak", []byte(`{}`)))
	got, err := NewStore(objs).Stale(ctx, t0, time.Hour, time.Minute)
	require.NoError(t, err)
	require.Empty(t, got)
}

func TestStaleRejectsBadThresholds(t *testing.T) {
	s := NewDirStore(t.TempDir())
	_, err := s.Stale(context.Background(), t0, 0, time.Minute)
	require.Error(t, err)
	_, err = s.Stale(context.Background(), t0, time.Hour, -time.Minute)
	require.Error(t, err)
}

func TestValidOutcome(t *testing.T) {
	for _, o := range []string{"success", "failure", "cancelled", "stack_busy", "never_reported", "never_started", "invalid_config"} {
		require.True(t, ValidOutcome(o), o)
	}
	for _, o := range []string{"", "ok", "SUCCESS", "success\n", "never"} {
		require.False(t, ValidOutcome(o), o)
	}
}
