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

// ReleasedPage is a PageWindow page handed to the send callback on release.
// Items are the page's items the stream has not touched, in read order.
// Cursor is the caller's resume position for the page (for example a
// DynamoDB Scan's LastEvaluatedKey), carried through untouched. High is the
// time the page was read, as passed to Hold.
type ReleasedPage[T, C any] struct {
	Segment int
	Items   []T
	Cursor  C
	High    time.Time
}

type windowPage[T, C any] struct {
	segment  int
	held     bool
	high     time.Time
	cursor   C
	touched  map[string]struct{}
	items    []T
	index    map[string]int
	dropped  []bool
	live     int
	released chan struct{}
}

// PageWindow stops a snapshot item from being emitted after a newer stream
// event for the same key. It is for connectors whose change stream has no
// single global log position to compare a snapshot read against, such as a
// per-shard stream. One PageWindow covers one table.
//
// A page is the items of one snapshot read request. A segment is a disjoint
// slice of the table's key space (for example a parallel scan segment), and
// it has at most one page at a time. T is the item type. C is the caller's
// opaque resume cursor, returned unchanged on release. Each item has a
// caller-encoded string key that identifies its primary key unambiguously.
//
// A page goes through these steps:
//
//   - Begin opens the page before the read request is sent.
//   - Touch records each stream key while the request is in flight.
//   - Hold attaches the read's items and skips the keys already touched.
//     After Hold, Touch drops the matching item from the page.
//   - ReleaseReady sends the page when the caller's predicate accepts its
//     read time.
//
// Abort discards a page that was never held, and Reset discards all pages.
//
// Ordering does not depend on clocks. The caller must call Touch before it
// enqueues the stream record, and ReleaseReady calls send while it holds the
// window's lock. Touch takes the same lock. So for each key, either the
// stream record drops the snapshot item, or the snapshot item is enqueued
// first. The release predicate only makes the order better. If it accepts a
// page too early, an older stream record can arrive after the snapshot item.
// This is a brief reorder and no key is lost. When each stream record
// carries the key's full new image, the last value for each key is also
// still the newest.
//
// Lock rule: send runs while the window's lock is held, and all methods
// take that lock. Do not call a method while you hold a lock that send, or
// code that send waits on, also takes. This causes a deadlock.
type PageWindow[T, C any] struct {
	mu    sync.Mutex
	pages map[int]*windowPage[T, C] // at most one per segment
}

// NewPageWindow returns an empty PageWindow.
func NewPageWindow[T, C any]() *PageWindow[T, C] {
	return &PageWindow[T, C]{pages: map[int]*windowPage[T, C]{}}
}

// Begin opens a page for segment, discarding any open page of that segment
// that was never held (a failed or empty request). A segment's previous held
// page must have been released first: Begin panics otherwise.
func (w *PageWindow[T, C]) Begin(segment int) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if p, exists := w.pages[segment]; exists && p.held {
		panic("incrementalsnapshot: PageWindow.Begin with a held page outstanding")
	}
	w.pages[segment] = &windowPage[T, C]{segment: segment, touched: map[string]struct{}{}}
}

// Hold attaches a read's items to the segment's open page, skipping keys
// already touched. keys[i] is items[i]'s key, so keys must have the same
// length as items, and the cursor is returned unchanged in the page's
// ReleasedPage. A held page stops recording
// touches and instead drops touched items. The returned channel closes once
// the page is released or the window is reset; the int is the number of
// items skipped. A page with no items left after the skip is not held: it is
// discarded at once and the returned channel is already closed, since it
// has nothing to order. Hold panics if the segment has no open, unheld page.
func (w *PageWindow[T, C]) Hold(segment int, items []T, keys []string, high time.Time, cursor C) (<-chan struct{}, int) {
	w.mu.Lock()
	defer w.mu.Unlock()
	p, exists := w.pages[segment]
	if !exists || p.held {
		panic("incrementalsnapshot: PageWindow.Hold without Begin")
	}
	p.held = true
	p.high = high
	p.cursor = cursor
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
	if p.live == 0 {
		delete(w.pages, segment)
		close(p.released)
	}
	return p.released, dropped
}

// Touch records a streamed key against every open page and drops it from
// held ones. It reports how many held items it dropped. Callers must call it
// before enqueueing the stream record that carries key.
func (w *PageWindow[T, C]) Touch(key string) int {
	w.mu.Lock()
	defer w.mu.Unlock()
	dropped := 0
	for _, p := range w.pages {
		if !p.held {
			p.touched[key] = struct{}{}
			continue
		}
		if i, exists := p.index[key]; exists {
			delete(p.index, key)
			p.dropped[i] = true
			p.live--
			dropped++
		}
	}
	return dropped
}

// Abort discards the segment's open page if it was never held.
func (w *PageWindow[T, C]) Abort(segment int) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if p, exists := w.pages[segment]; exists && !p.held {
		delete(w.pages, segment)
	}
}

// ReleaseReady sends every held page whose high canRelease accepts, holding
// the window's lock across send. Pages are released in no particular order
// (map iteration order): segments cover disjoint keys and each holds at most
// one page, so no two released pages carry the same key. A send error stops
// the pass and is returned; the failing page stays held.
func (w *PageWindow[T, C]) ReleaseReady(canRelease func(high time.Time) bool, send func(ReleasedPage[T, C]) error) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	for seg, p := range w.pages {
		if !p.held || !canRelease(p.high) {
			continue
		}
		items := make([]T, 0, p.live)
		for i, it := range p.items {
			if !p.dropped[i] {
				items = append(items, it)
			}
		}
		if err := send(ReleasedPage[T, C]{Segment: seg, Items: items, Cursor: p.cursor, High: p.high}); err != nil {
			return err
		}
		delete(w.pages, seg)
		close(p.released)
	}
	return nil
}

// HeldItems counts live items across held pages.
func (w *PageWindow[T, C]) HeldItems() int {
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

// OldestHeld returns the earliest read time among held pages, and false
// when no page is held.
func (w *PageWindow[T, C]) OldestHeld() (time.Time, bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	var (
		oldest time.Time
		found  bool
	)
	for _, p := range w.pages {
		if p.held && (!found || p.high.Before(oldest)) {
			oldest, found = p.high, true
		}
	}
	return oldest, found
}

// Reset discards every page, closing held pages' released channels, so a
// failed or cancelled backfill leaves no stale page for the next run.
func (w *PageWindow[T, C]) Reset() {
	w.mu.Lock()
	defer w.mu.Unlock()
	for seg, p := range w.pages {
		if p.held {
			close(p.released)
		}
		delete(w.pages, seg)
	}
}
