// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package incrementalsnapshot

import (
	"fmt"
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
// RefreshDone that lists only known shards, or until it has stayed unsettled
// for idleGrace; after that, CaughtUpPast judges the shards it knows.
// Releasing then is safe for the same reason a wrong idle guess is: it can
// only cost ordering, never a stale value.
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
	// unsettledSince is when the tracker last went from settled to
	// unsettled (or was created). Unsettled state is bounded by idleGrace:
	// a described shard that never gets a reader (for example one whose
	// iterator keeps failing) would otherwise hold every page forever.
	unsettledSince time.Time
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

		unsettledSince: now(),
	}
}

func (p *ShardProgress) unsettleLocked() {
	if !p.unsettled {
		p.unsettled = true
		p.unsettledSince = p.now()
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
	p.unsettleLocked()
}

// RefreshDone settles the tracker if every described shard is known, and
// unsettles it otherwise. Exhausted shards the refresh no longer describes
// (trimmed from the stream) are forgotten, so a long-running stream does not
// accumulate every shard it has ever read.
func (p *ShardProgress) RefreshDone(described []string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	listed := make(map[string]struct{}, len(described))
	for _, id := range described {
		listed[id] = struct{}{}
	}
	for id, m := range p.shards {
		if _, ok := listed[id]; m.exhausted && !ok {
			delete(p.shards, id)
		}
	}
	for _, id := range described {
		if _, exists := p.shards[id]; !exists {
			p.unsettleLocked()
			return
		}
	}
	p.unsettled = false
}

// waitingLocked reports whether the tracker is unsettled and still within
// idleGrace, so nothing counts as caught up.
func (p *ShardProgress) waitingLocked() bool {
	return p.unsettled && p.now().Sub(p.unsettledSince) < p.idleGrace
}

// CaughtUpPast reports whether the tracker is settled (or has been unsettled
// for at least idleGrace) and every live shard has been read past t.
func (p *ShardProgress) CaughtUpPast(t time.Time) bool {
	return p.CaughtUpPredicate()(t)
}

// CaughtUpPredicate returns CaughtUpPast evaluated against the tracker's
// current state, which it reads once under the lock: one release pass checks
// many pages against a single walk of the shards.
func (p *ShardProgress) CaughtUpPredicate() func(t time.Time) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.waitingLocked() {
		return func(time.Time) bool { return false }
	}
	var (
		oldest  time.Time
		hasLive bool
	)
	for _, m := range p.shards {
		if m.exhausted {
			continue
		}
		if !hasLive || m.seen.Before(oldest) {
			oldest = m.seen
			hasLive = true
		}
	}
	return func(t time.Time) bool { return !hasLive || !oldest.Before(t) }
}

// BlockingReason describes why CaughtUpPast(t) is false, for logs, or
// returns "" when it is true.
func (p *ShardProgress) BlockingReason(t time.Time) string {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.waitingLocked() {
		return fmt.Sprintf("waiting for a shard refresh to register every shard of the stream (unsettled for %s, judged anyway after %s)",
			p.now().Sub(p.unsettledSince).Round(time.Second), p.idleGrace)
	}
	var (
		behind  []string
		laggard string
		oldest  time.Time
	)
	for id, m := range p.shards {
		if m.exhausted || !m.seen.Before(t) {
			continue
		}
		behind = append(behind, id)
		if laggard == "" || m.seen.Before(oldest) {
			laggard, oldest = id, m.seen
		}
	}
	if laggard == "" {
		return ""
	}
	seen := "nothing yet"
	if !oldest.IsZero() {
		seen = oldest.UTC().Format(time.RFC3339)
	}
	return fmt.Sprintf("%d shard(s) not yet read past %s; furthest behind is %s, read through %s",
		len(behind), t.UTC().Format(time.RFC3339), laggard, seen)
}
