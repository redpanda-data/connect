// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package incrementalsnapshot

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

var t0 = time.Date(2026, 9, 29, 12, 0, 0, 0, time.UTC)

const testIdleGrace = time.Minute

func settled(p *ShardProgress, ids ...string) {
	for _, id := range ids {
		p.Register(id)
	}
	p.RefreshDone(ids)
}

func TestShardProgressUnsettledUntilFirstRefresh(t *testing.T) {
	p := NewShardProgress(2*time.Second, testIdleGrace, time.Now)
	assert.False(t, p.CaughtUpPast(t0), "no refresh yet")
	p.RefreshDone(nil)
	assert.True(t, p.CaughtUpPast(t0), "a settled stream with no shards is caught up")
}

func TestShardProgressRegisteredShardBlocksUntilObserved(t *testing.T) {
	p := NewShardProgress(2*time.Second, testIdleGrace, time.Now)
	settled(p, "s1")
	assert.False(t, p.CaughtUpPast(t0))
	p.Observe("s1", t0.Add(5*time.Second))
	assert.True(t, p.CaughtUpPast(t0.Add(5*time.Second)))
	assert.False(t, p.CaughtUpPast(t0.Add(6*time.Second)))
}

func TestShardProgressIdleNeedsTwoEmptyPollsAfterRecords(t *testing.T) {
	p := NewShardProgress(2*time.Second, testIdleGrace, time.Now)
	settled(p, "s1")
	p.Observe("s1", t0)
	p.ObserveIdle("s1", t0.Add(10*time.Second))
	assert.False(t, p.CaughtUpPast(t0.Add(8*time.Second)), "one empty poll is not proof")
	p.ObserveIdle("s1", t0.Add(11*time.Second))
	assert.True(t, p.CaughtUpPast(t0.Add(9*time.Second)), "second empty poll advances to pollStart - margin")
	p.Observe("s1", t0.Add(12*time.Second))
	p.ObserveIdle("s1", t0.Add(20*time.Second))
	assert.False(t, p.CaughtUpPast(t0.Add(18*time.Second)), "a record resets the empty streak")
}

func TestShardProgressNeverProductiveShardUsesGrace(t *testing.T) {
	p := NewShardProgress(2*time.Second, testIdleGrace, time.Now)
	settled(p, "s1")
	p.ObserveIdle("s1", t0)
	p.ObserveIdle("s1", t0.Add(30*time.Second))
	assert.False(t, p.CaughtUpPast(t0), "within grace")
	p.ObserveIdle("s1", t0.Add(testIdleGrace))
	assert.True(t, p.CaughtUpPast(t0.Add(testIdleGrace-2*time.Second)))
}

func TestShardProgressExhaustedUnsettlesUntilChildrenRegistered(t *testing.T) {
	p := NewShardProgress(2*time.Second, testIdleGrace, time.Now)
	settled(p, "parent")
	p.Observe("parent", t0.Add(time.Hour))
	p.Exhausted("parent")
	assert.False(t, p.CaughtUpPast(t0), "unsettled after exhaustion")

	// Refresh lists a child that failed to prepare: still unsettled.
	p.RefreshDone([]string{"parent", "child"})
	assert.False(t, p.CaughtUpPast(t0))

	p.Register("child")
	p.RefreshDone([]string{"parent", "child"})
	assert.False(t, p.CaughtUpPast(t0), "child not yet polled")
	p.Observe("child", t0.Add(time.Minute))
	assert.True(t, p.CaughtUpPast(t0.Add(time.Minute)), "exhausted parent no longer counts")
}

func TestShardProgressReRegisterKeepsExhausted(t *testing.T) {
	p := NewShardProgress(0, testIdleGrace, time.Now)
	settled(p, "s1")
	p.Exhausted("s1")
	p.Register("s1")
	p.RefreshDone([]string{"s1"})
	assert.True(t, p.CaughtUpPast(t0.Add(time.Hour)))
}

// TestShardProgressUnsettledBoundedByIdleGrace: a described shard that never
// gets a reader (its iterator keeps failing to prepare) must not hold pages
// forever. After idleGrace of continuous unsettled state the tracker judges
// the shards it knows; a later settle and re-unsettle restarts the clock.
func TestShardProgressUnsettledBoundedByIdleGrace(t *testing.T) {
	now := t0
	p := NewShardProgress(0, testIdleGrace, func() time.Time { return now })
	p.Register("s1")
	p.Observe("s1", t0.Add(time.Hour))

	for range 3 {
		p.RefreshDone([]string{"s1", "never-prepared"})
		now = now.Add(testIdleGrace / 4)
	}
	assert.False(t, p.CaughtUpPast(t0), "within grace the unknown shard still blocks")

	now = t0.Add(testIdleGrace)
	p.RefreshDone([]string{"s1", "never-prepared"})
	assert.True(t, p.CaughtUpPast(t0), "unsettled past idleGrace judges the known shards")
	assert.False(t, p.CaughtUpPast(t0.Add(2*time.Hour)), "known shards must still be caught up")

	// Settling and then unsettling again restarts the grace clock.
	p.RefreshDone([]string{"s1"})
	p.Exhausted("s1")
	p.Register("s2")
	assert.False(t, p.CaughtUpPast(t0), "a fresh unsettle waits a full grace again")
	now = now.Add(testIdleGrace)
	p.Observe("s2", t0.Add(time.Hour))
	assert.True(t, p.CaughtUpPast(t0))
}

// TestShardProgressInitialUnsettledBoundedByIdleGrace: a first refresh that
// never succeeds also stops blocking after idleGrace.
func TestShardProgressInitialUnsettledBoundedByIdleGrace(t *testing.T) {
	now := t0
	p := NewShardProgress(0, testIdleGrace, func() time.Time { return now })
	assert.False(t, p.CaughtUpPast(t0))
	now = now.Add(testIdleGrace)
	assert.True(t, p.CaughtUpPast(t0))
}

// TestShardProgressForgetsTrimmedExhaustedShards: an exhausted shard that a
// refresh no longer describes is forgotten, so a long-running stream does
// not keep, and scan on every release, every shard it has ever read. One
// still described stays known, so the refresh still settles.
func TestShardProgressForgetsTrimmedExhaustedShards(t *testing.T) {
	p := NewShardProgress(0, testIdleGrace, time.Now)
	settled(p, "old", "parent", "child")
	p.Exhausted("old")
	p.Exhausted("parent")
	p.RefreshDone([]string{"parent", "child"})

	p.mu.Lock()
	_, hasOld := p.shards["old"]
	_, hasParent := p.shards["parent"]
	_, hasChild := p.shards["child"]
	settledNow := !p.unsettled
	p.mu.Unlock()
	assert.False(t, hasOld, "a trimmed exhausted shard is forgotten")
	assert.True(t, hasParent, "a described exhausted shard is kept")
	assert.True(t, hasChild)
	assert.True(t, settledNow)
}

func TestShardProgressKeepsUndescribedLiveShards(t *testing.T) {
	p := NewShardProgress(0, testIdleGrace, time.Now)
	settled(p, "s1")
	p.RefreshDone(nil)
	assert.False(t, p.CaughtUpPast(t0), "a live shard still blocks even if a refresh missed it")
}

func TestShardProgressCaughtUpPredicateMatchesCaughtUpPast(t *testing.T) {
	p := NewShardProgress(0, testIdleGrace, time.Now)
	assert.False(t, p.CaughtUpPredicate()(t0), "unsettled")
	settled(p, "s1", "s2")
	p.Observe("s1", t0.Add(10*time.Second))
	p.Observe("s2", t0.Add(5*time.Second))
	caughtUp := p.CaughtUpPredicate()
	for _, at := range []time.Duration{0, 5 * time.Second, 6 * time.Second, 10 * time.Second} {
		assert.Equal(t, p.CaughtUpPast(t0.Add(at)), caughtUp(t0.Add(at)), "at %s", at)
	}
	assert.True(t, caughtUp(t0.Add(5*time.Second)))
	assert.False(t, caughtUp(t0.Add(6*time.Second)))
}

func TestShardProgressBlockingReason(t *testing.T) {
	p := NewShardProgress(0, testIdleGrace, time.Now)
	assert.Contains(t, p.BlockingReason(t0), "shard refresh", "unsettled")
	settled(p, "s1", "s2")
	p.Observe("s1", t0.Add(10*time.Second))
	reason := p.BlockingReason(t0.Add(time.Second))
	assert.Contains(t, reason, "1 shard(s)")
	assert.Contains(t, reason, "s2")
	assert.Contains(t, reason, "nothing yet")
	p.Observe("s2", t0.Add(10*time.Second))
	assert.Empty(t, p.BlockingReason(t0.Add(time.Second)), "nothing blocks")
}
