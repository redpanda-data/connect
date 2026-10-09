// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

// Package incrementalsnapshot drives the aws_dynamodb_cdc input's incremental
// snapshot: it backfills a table page by page while the table streams, and
// releases each page only once no stream record can still supersede it out
// of order. The connector-neutral machinery (the page window, shard progress
// and table queue) lives in internal/replication/incrementalsnapshot; this
// package binds it to DynamoDB items and stream records, and reaches the
// input's checkpoint store, scanner and message channel through Deps.
package incrementalsnapshot

import (
	"time"

	dynamodbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	streamstypes "github.com/aws/aws-sdk-go-v2/service/dynamodbstreams/types"

	"github.com/redpanda-data/connect/v4/internal/replication/incrementalsnapshot"
)

// Item is one DynamoDB item, or a primary key in item form.
type Item = map[string]dynamodbtypes.AttributeValue

// Window is a table's snapshot page window: items are scanned DynamoDB
// items, and each page's cursor is its Scan's LastEvaluatedKey.
type Window = incrementalsnapshot.PageWindow[Item, Item]

// ReleasedPage is a page Window hands to its send callback.
type ReleasedPage = incrementalsnapshot.ReleasedPage[Item, Item]

// State is one table's incremental snapshot machinery: the page window and
// the shard progress tracker. Readers and shard refresh call its methods;
// every reader hook is a no-op on a nil receiver, so tables outside
// snapshot_mode: incremental pay nothing.
type State struct {
	window   *Window
	progress *incrementalsnapshot.ShardProgress
	margin   time.Duration
	// nudge wakes the release loop early after progress moves.
	nudge chan struct{}
}

// NewState returns an empty State. margin is snapshot_watermark_margin and
// idleGrace is snapshot_idle_shard_grace.
func NewState(margin, idleGrace time.Duration) *State {
	return &State{
		window:   incrementalsnapshot.NewPageWindow[Item, Item](),
		progress: incrementalsnapshot.NewShardProgress(margin, idleGrace, time.Now),
		margin:   margin,
		nudge:    make(chan struct{}, 1),
	}
}

// Window returns the table's page window.
func (s *State) Window() *Window {
	return s.window
}

// Progress returns the table's shard progress tracker.
func (s *State) Progress() *incrementalsnapshot.ShardProgress {
	return s.progress
}

// Poke wakes the release loop without waiting for its next tick. It never
// blocks.
func (s *State) Poke() {
	select {
	case s.nudge <- struct{}{}:
	default:
	}
}

// TouchRecords drops every held or in-flight snapshot item the records'
// keys touch. Readers call it before enqueueing the records.
func (s *State) TouchRecords(records []streamstypes.Record) int {
	if s == nil {
		return 0
	}
	dropped := 0
	for _, r := range records {
		if r.Dynamodb == nil {
			continue
		}
		if key, ok := WindowKeyFromStream(r.Dynamodb.Keys); ok {
			dropped += s.window.Touch(key)
		}
	}
	return dropped
}

// ObserveRecords advances the shard to the newest record time. Readers call
// it after the records have been enqueued.
func (s *State) ObserveRecords(shardID string, records []streamstypes.Record) {
	if s == nil {
		return
	}
	for _, r := range records {
		if r.Dynamodb != nil && r.Dynamodb.ApproximateCreationDateTime != nil {
			s.progress.Observe(shardID, *r.Dynamodb.ApproximateCreationDateTime)
		}
	}
	s.Poke()
}

// ObserveIdle records a poll of shardID, started at pollStart, that returned
// no records.
func (s *State) ObserveIdle(shardID string, pollStart time.Time) {
	if s == nil {
		return
	}
	s.progress.ObserveIdle(shardID, pollStart)
	s.Poke()
}

// Exhausted records that shardID is closed and fully read.
func (s *State) Exhausted(shardID string) {
	if s == nil {
		return
	}
	s.progress.Exhausted(shardID)
}

// Register starts tracking shardID.
func (s *State) Register(shardID string) {
	if s == nil {
		return
	}
	s.progress.Register(shardID)
}

// RefreshDone records a completed shard refresh that described the given
// shards.
func (s *State) RefreshDone(described []string) {
	if s == nil {
		return
	}
	s.progress.RefreshDone(described)
	s.Poke()
}

// CanRelease reports whether a page read up to high may be released.
func (s *State) CanRelease(high time.Time) bool {
	// The one second is a protocol constant, not a tuning knob: DynamoDB
	// Streams rounds ApproximateCreationDateTime down to the second, so a
	// shard seen up to second S may still deliver records from later in S.
	// User-tunable slack is snapshot_watermark_margin.
	return s.progress.CaughtUpPast(high.Add(time.Second + s.margin))
}
