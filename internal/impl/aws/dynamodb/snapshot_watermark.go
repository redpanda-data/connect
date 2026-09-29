// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package dynamodb

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

// shardProgress tracks, per shard of one table stream, how far the stream
// has been read. It only drives the incremental snapshot's ordering
// refinement: a wrong answer releases a page early, which can briefly
// reorder an older stream value after the snapshot row but never loses or
// stales a key (see the spec's Safety rule).
type shardProgress struct {
	mu     sync.Mutex
	margin time.Duration
	// idleGrace is how long a shard that has never returned a record must
	// keep returning empty pages before it is treated as idle. GetRecords can
	// return empty pages before reaching a shard's data, so emptiness alone is
	// not proof; without a grace a genuinely idle new shard would hold every
	// incremental snapshot page of its table forever.
	idleGrace time.Duration
	now       func() time.Time
	shards    map[string]*shardMark
	unsettled bool
}

func newShardProgress(margin, idleGrace time.Duration, now func() time.Time) *shardProgress {
	return &shardProgress{
		margin:    margin,
		idleGrace: idleGrace,
		now:       now,
		shards:    map[string]*shardMark{},
		unsettled: true, // until the first refresh completes
	}
}

func (p *shardProgress) markLocked(id string) *shardMark {
	m, ok := p.shards[id]
	if !ok {
		m = &shardMark{}
		p.shards[id] = m
	}
	return m
}

// Register records a shard that has a reader. An already known shard,
// including an exhausted one, is left as it is.
func (p *shardProgress) Register(id string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.markLocked(id)
}

// Observe advances a shard to a record's creation time.
func (p *shardProgress) Observe(id string, recordTime time.Time) {
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

// ObserveIdle records an empty GetRecords page for a poll that started at
// pollStart.
func (p *shardProgress) ObserveIdle(id string, pollStart time.Time) {
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
// until a refresh has registered every shard it lists (the split children).
func (p *shardProgress) Exhausted(id string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.markLocked(id).exhausted = true
	p.unsettled = true
}

// RefreshDone settles the tracker if every described shard is known.
func (p *shardProgress) RefreshDone(described []string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, id := range described {
		if _, ok := p.shards[id]; !ok {
			p.unsettled = true
			return
		}
	}
	p.unsettled = false
}

// CaughtUpPast reports whether every live shard has been read past t.
func (p *shardProgress) CaughtUpPast(t time.Time) bool {
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
