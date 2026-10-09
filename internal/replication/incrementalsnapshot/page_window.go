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

// PageWindow holds one table's open snapshot pages for a connector whose
// stream has no single global log position to compare a snapshot read
// against, such as a per-shard change stream. T is the item type and C the
// caller's opaque resume cursor; keys are caller-encoded strings, one per
// item, and must identify an item's primary key unambiguously.
//
// A segment is a disjoint slice of the table's key space (for example a
// parallel scan segment) and holds at most one page at a time. A page opens
// (Begin) before its read request, so a stream record processed while the
// request is in flight still drops the key once the items arrive (Hold).
// Held pages are sent (ReleaseReady) only once the caller's predicate
// accepts the page's read time.
//
// The window is clock-free in its safety: it never decides ordering from
// timestamps. Release sends while holding the window's lock, and Touch
// takes the same lock before the caller enqueues the stream record, so for
// any key either the stream record drops the snapshot item or the item is
// enqueued first. A snapshot item can therefore never be emitted after the
// stream event for its key. The release predicate only refines ordering: a
// wrong answer releases a page early, which can briefly reorder an older
// stream value after the snapshot item but never loses or stales a key.
//
// Lock order: send runs under the window's lock, and Touch, Begin and the
// other methods take it. A caller must never call into the window while
// holding a lock that send (or any code send waits on) also acquires.
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
// items skipped. Hold panics if the segment has no open, unheld page.
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
