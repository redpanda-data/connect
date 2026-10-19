// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package incrementalsnapshot

import (
	"sync"
	"time"
)

type shardMark struct {
	seen        time.Time
	exhausted   bool
	productive  bool // has returned at least one record
	emptyStreak int
	firstEmpty  time.Time
}

// ShardProgress tracks, per shard of one table's change stream, how far the
// stream has been read, for streams split into shards that each carry their
// own position (for example DynamoDB Streams shards). It only drives a
// PageWindow's ordering refinement: a wrong answer releases a page early,
// which can briefly reorder an older stream value after the snapshot item
// but never loses or stales a key (see PageWindow).
//
// The tracker starts unsettled and reports nothing caught up until the first
// RefreshDone that lists only known shards.
type ShardProgress struct {
	mu     sync.Mutex
	margin time.Duration
	// idleGrace is how long a shard that has never returned a record must
	// keep returning empty reads before it is treated as idle. A stream can
	// return empty reads before reaching a shard's data, so emptiness alone
	// is not proof; without a grace a genuinely idle new shard would hold
	// every snapshot page of its table forever.
	idleGrace time.Duration
	now       func() time.Time
	shards    map[string]*shardMark
	unsettled bool
}

// NewShardProgress returns an unsettled tracker. margin is subtracted from
// an idle shard's poll start before it counts as read up to that time;
// idleGrace bounds how long a never-productive shard must stay empty before
// it counts as idle.
func NewShardProgress(margin, idleGrace time.Duration, now func() time.Time) *ShardProgress {
	return &ShardProgress{
		margin:    margin,
		idleGrace: idleGrace,
		now:       now,
		shards:    map[string]*shardMark{},
		unsettled: true, // until the first refresh completes
	}
}

func (p *ShardProgress) markLocked(id string) *shardMark {
	m, exists := p.shards[id]
	if !exists {
		m = &shardMark{}
		p.shards[id] = m
	}
	return m
}

// Register records a shard that has a reader. An already known shard,
// including an exhausted one, is left as it is.
func (p *ShardProgress) Register(id string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.markLocked(id)
}

// Observe advances a shard to a record's creation time.
func (p *ShardProgress) Observe(id string, recordTime time.Time) {
	p.mu.Lock()
	defer p.mu.Unlock()
	m := p.markLocked(id)
	m.productive = true
	m.emptyStreak = 0
	m.firstEmpty = time.Time{}
	if recordTime.After(m.seen) {
		m.seen = recordTime
	}
}

// ObserveIdle records an empty read (for example an empty DynamoDB
// GetRecords page) for a poll that started at pollStart. A shard that has
// returned records counts as idle after two consecutive empty reads; one
// that never has, after idleGrace of empty reads. An idle shard advances to
// pollStart minus margin.
func (p *ShardProgress) ObserveIdle(id string, pollStart time.Time) {
	p.mu.Lock()
	defer p.mu.Unlock()
	m := p.markLocked(id)
	m.emptyStreak++
	if m.firstEmpty.IsZero() {
		m.firstEmpty = pollStart
	}
	idle := (m.productive && m.emptyStreak >= 2) ||
		(!m.productive && pollStart.Sub(m.firstEmpty) >= p.idleGrace)
	if !idle {
		return
	}
	if at := pollStart.Add(-p.margin); at.After(m.seen) {
		m.seen = at
	}
}

// Exhausted removes a closed shard from the check and unsettles the tracker
// until a refresh has registered every shard it lists (for example the
// children of a split shard).
func (p *ShardProgress) Exhausted(id string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.markLocked(id).exhausted = true
	p.unsettled = true
}

// RefreshDone settles the tracker if every described shard is known, and
// unsettles it otherwise.
func (p *ShardProgress) RefreshDone(described []string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, id := range described {
		if _, exists := p.shards[id]; !exists {
			p.unsettled = true
			return
		}
	}
	p.unsettled = false
}

// CaughtUpPast reports whether the tracker is settled and every live shard
// has been read past t.
func (p *ShardProgress) CaughtUpPast(t time.Time) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.unsettled {
		return false
	}
	for _, m := range p.shards {
		if m.exhausted {
			continue
		}
		if m.seen.Before(t) {
			return false
		}
	}
	return true
}
