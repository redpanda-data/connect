// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package dynamodb

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

var t0 = time.Date(2026, 9, 29, 12, 0, 0, 0, time.UTC)

const testIdleGrace = time.Minute

func settled(p *shardProgress, ids ...string) {
	for _, id := range ids {
		p.Register(id)
	}
	p.RefreshDone(ids)
}

func TestShardProgressUnsettledUntilFirstRefresh(t *testing.T) {
	p := newShardProgress(2*time.Second, testIdleGrace, time.Now)
	assert.False(t, p.CaughtUpPast(t0), "no refresh yet")
	p.RefreshDone(nil)
	assert.True(t, p.CaughtUpPast(t0), "a settled stream with no shards is caught up")
}

func TestShardProgressRegisteredShardBlocksUntilObserved(t *testing.T) {
	p := newShardProgress(2*time.Second, testIdleGrace, time.Now)
	settled(p, "s1")
	assert.False(t, p.CaughtUpPast(t0))
	p.Observe("s1", t0.Add(5*time.Second))
	assert.True(t, p.CaughtUpPast(t0.Add(5*time.Second)))
	assert.False(t, p.CaughtUpPast(t0.Add(6*time.Second)))
}

func TestShardProgressIdleNeedsTwoEmptyPollsAfterRecords(t *testing.T) {
	p := newShardProgress(2*time.Second, testIdleGrace, time.Now)
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
	p := newShardProgress(2*time.Second, testIdleGrace, time.Now)
	settled(p, "s1")
	p.ObserveIdle("s1", t0)
	p.ObserveIdle("s1", t0.Add(30*time.Second))
	assert.False(t, p.CaughtUpPast(t0), "within grace")
	p.ObserveIdle("s1", t0.Add(testIdleGrace))
	assert.True(t, p.CaughtUpPast(t0.Add(testIdleGrace-2*time.Second)))
}

func TestShardProgressExhaustedUnsettlesUntilChildrenRegistered(t *testing.T) {
	p := newShardProgress(2*time.Second, testIdleGrace, time.Now)
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
	p := newShardProgress(0, testIdleGrace, time.Now)
	settled(p, "s1")
	p.Exhausted("s1")
	p.Register("s1")
	p.RefreshDone([]string{"s1"})
	assert.True(t, p.CaughtUpPast(t0.Add(time.Hour)))
}
