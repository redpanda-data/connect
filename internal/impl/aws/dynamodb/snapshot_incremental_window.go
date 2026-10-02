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

	dynamodbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// releasedPage is a window page handed to the send callback on release.
type releasedPage struct {
	Segment int
	Items   DynamoItems
	LastKey map[string]dynamodbtypes.AttributeValue
	High    time.Time
}

type windowPage struct {
	segment  int
	held     bool
	high     time.Time
	lastKey  map[string]dynamodbtypes.AttributeValue
	touched  map[string]struct{}
	items    DynamoItems
	index    map[string]int
	dropped  []bool
	live     int
	released chan struct{}
}

// incrementalWindow holds one table's open incremental snapshot pages. A
// page opens (Begin) before its Scan request, so a stream record processed
// while the request is in flight still drops the key once the items arrive
// (Hold). Release sends while holding mu, and Touch takes mu before the
// reader enqueues, so for any key either the stream record drops the item
// or the item is enqueued first.
type incrementalWindow struct {
	mu    sync.Mutex
	pages map[int]*windowPage // at most one per segment
}

func newIncrementalWindow() *incrementalWindow {
	return &incrementalWindow{pages: map[int]*windowPage{}}
}

// Begin opens a page for segment, discarding any open page of that segment
// that was never held (a failed or empty request). A segment's previous held
// page must have been released first.
func (w *incrementalWindow) Begin(segment int) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if p, ok := w.pages[segment]; ok && p.held {
		panic("incrementalWindow: Begin with a held page outstanding")
	}
	w.pages[segment] = &windowPage{segment: segment, touched: map[string]struct{}{}}
}

// Hold attaches a Scan's items to the segment's open page, skipping keys
// already touched. The returned channel closes once the page is released.
func (w *incrementalWindow) Hold(segment int, items DynamoItems, keys []string, high time.Time, lastKey map[string]dynamodbtypes.AttributeValue) (<-chan struct{}, int) {
	w.mu.Lock()
	defer w.mu.Unlock()
	p, ok := w.pages[segment]
	if !ok || p.held {
		panic("incrementalWindow: Hold without Begin")
	}
	p.held = true
	p.high = high
	p.lastKey = lastKey
	p.index = make(map[string]int, len(items))
	p.released = make(chan struct{})
	dropped := 0
	for i, it := range items {
		if _, t := p.touched[keys[i]]; t {
			dropped++
			continue
		}
		p.index[keys[i]] = len(p.items)
		p.items = append(p.items, it)
		p.dropped = append(p.dropped, false)
		p.live++
	}
	p.touched = nil // touched is dead state after Hold; free memory
	return p.released, dropped
}

// Touch records a streamed key against every open page and drops it from
// held ones. It reports how many held items it dropped.
func (w *incrementalWindow) Touch(key string) int {
	w.mu.Lock()
	defer w.mu.Unlock()
	dropped := 0
	for _, p := range w.pages {
		if !p.held {
			p.touched[key] = struct{}{}
			continue
		}
		if i, ok := p.index[key]; ok {
			delete(p.index, key)
			p.dropped[i] = true
			p.live--
			dropped++
		}
	}
	return dropped
}

// Abort discards the segment's open page if it was never held.
func (w *incrementalWindow) Abort(segment int) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if p, ok := w.pages[segment]; ok && !p.held {
		delete(w.pages, segment)
	}
}

// ReleaseReady sends every held page whose high canRelease accepts, holding
// mu across send. Pages are released in no particular order: segments cover
// disjoint keys and each holds at most one page, so no two released pages
// carry the same key. A send error stops the pass and is returned; the
// failing page stays held.
func (w *incrementalWindow) ReleaseReady(canRelease func(high time.Time) bool, send func(releasedPage) error) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	for seg, p := range w.pages {
		if !p.held || !canRelease(p.high) {
			continue
		}
		items := make(DynamoItems, 0, p.live)
		for i, it := range p.items {
			if !p.dropped[i] {
				items = append(items, it)
			}
		}
		if err := send(releasedPage{Segment: seg, Items: items, LastKey: p.lastKey, High: p.high}); err != nil {
			return err
		}
		delete(w.pages, seg)
		close(p.released)
	}
	return nil
}

// HeldItems counts live items across held pages.
func (w *incrementalWindow) HeldItems() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	n := 0
	for _, p := range w.pages {
		if p.held {
			n += p.live
		}
	}
	return n
}

// Reset discards every page, closing held pages' released channels, so a
// failed or cancelled backfill leaves no stale page for the next run.
func (w *incrementalWindow) Reset() {
	w.mu.Lock()
	defer w.mu.Unlock()
	for seg, p := range w.pages {
		if p.held {
			close(p.released)
		}
		delete(w.pages, seg)
	}
}
