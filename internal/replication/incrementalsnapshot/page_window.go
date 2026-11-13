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

// keyedBuffer keeps items in insertion order and removes one by key in O(1).
// Remove marks the slot instead of shifting items, so draining a buffer one
// key at a time stays linear. Keys must be unique: adding a key again leaves
// the earlier item in place but no longer removable by key.
type keyedBuffer[T any] struct {
	items   []T
	index   map[string]int
	removed []bool
	live    int
}

func newKeyedBuffer[T any](capacity int) keyedBuffer[T] {
	return keyedBuffer[T]{
		items:   make([]T, 0, capacity),
		index:   make(map[string]int, capacity),
		removed: make([]bool, 0, capacity),
	}
}

// Add appends item under key.
func (b *keyedBuffer[T]) Add(key string, item T) {
	b.index[key] = len(b.items)
	b.items = append(b.items, item)
	b.removed = append(b.removed, false)
	b.live++
}

// Remove removes the item under key and reports whether there was one.
func (b *keyedBuffer[T]) Remove(key string) bool {
	i, exists := b.index[key]
	if !exists {
		return false
	}
	delete(b.index, key)
	b.removed[i] = true
	b.live--
	return true
}

// Len returns the number of items not removed.
func (b *keyedBuffer[T]) Len() int { return b.live }

// Items returns the items not removed, in insertion order, as a new slice.
func (b *keyedBuffer[T]) Items() []T {
	out := make([]T, 0, b.live)
	for i, it := range b.items {
		if !b.removed[i] {
			out = append(out, it)
		}
	}
	return out
}

// heldPage is a page whose read has returned and that waits for release. Its
// methods do not lock: PageWindow calls them under its own lock.
type heldPage[T, C any] struct {
	items    keyedBuffer[T]
	high     time.Time
	cursor   C
	released chan struct{}
}

// holdPage builds the held page for a read's items, skipping every key in
// touched (keys streamed while the read was in flight). keys[i] is items[i]'s
// key. It returns the page and the number of items skipped.
func holdPage[T, C any](touched map[string]struct{}, items []T, keys []string, high time.Time, cursor C) (*heldPage[T, C], int) {
	p := &heldPage[T, C]{
		items:    newKeyedBuffer[T](len(items)),
		high:     high,
		cursor:   cursor,
		released: make(chan struct{}),
	}
	skipped := 0
	for i, it := range items {
		if _, t := touched[keys[i]]; t {
			skipped++
			continue
		}
		p.items.Add(keys[i], it)
	}
	return p, skipped
}

// touch drops the item under key and reports whether the page held one.
func (p *heldPage[T, C]) touch(key string) bool { return p.items.Remove(key) }

// liveItems returns the items the stream has not touched, in read order.
func (p *heldPage[T, C]) liveItems() []T { return p.items.Items() }

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
	mu sync.Mutex
	// open maps each segment whose read is in flight (after Begin, before
	// Hold) to the keys streamed since Begin.
	open map[int]map[string]struct{}
	// held maps each segment whose read has returned (after Hold) to its page
	// waiting for release. A segment is in at most one of open and held.
	held map[int]*heldPage[T, C]
}

// NewPageWindow returns an empty PageWindow.
func NewPageWindow[T, C any]() *PageWindow[T, C] {
	return &PageWindow[T, C]{
		open: map[int]map[string]struct{}{},
		held: map[int]*heldPage[T, C]{},
	}
}

// Begin opens a page for segment, discarding any open page of that segment
// that was never held (a failed or empty request). A segment's previous held
// page must have been released first: Begin panics otherwise.
func (w *PageWindow[T, C]) Begin(segment int) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if _, exists := w.held[segment]; exists {
		panic("incrementalsnapshot: PageWindow.Begin with a held page outstanding")
	}
	w.open[segment] = map[string]struct{}{}
}

// Hold attaches a read's items to the segment's open page and moves the page
// from open to held, skipping keys already touched. keys[i] is items[i]'s
// key, so keys must have the same length as items, and the cursor is
// returned unchanged in the page's ReleasedPage. After Hold, Touch drops
// touched items from the page. The returned channel closes once the page is
// released or the window is reset, and the int is the number of items
// skipped. A page with no items left after the skip is not held: it is
// discarded at once and the returned channel is already closed, since it has
// nothing to order. Hold panics if the segment has no open page.
func (w *PageWindow[T, C]) Hold(segment int, items []T, keys []string, high time.Time, cursor C) (<-chan struct{}, int) {
	w.mu.Lock()
	defer w.mu.Unlock()
	touched, exists := w.open[segment]
	if !exists {
		panic("incrementalsnapshot: PageWindow.Hold without Begin")
	}
	delete(w.open, segment)
	p, skipped := holdPage(touched, items, keys, high, cursor)
	if p.items.Len() == 0 {
		close(p.released)
	} else {
		w.held[segment] = p
	}
	return p.released, skipped
}

// Touch records a streamed key against every open page and drops it from
// held ones. It reports how many held items it dropped. Callers must call it
// before enqueueing the stream record that carries key.
func (w *PageWindow[T, C]) Touch(key string) int {
	w.mu.Lock()
	defer w.mu.Unlock()
	for _, touched := range w.open {
		touched[key] = struct{}{}
	}
	dropped := 0
	for _, p := range w.held {
		if p.touch(key) {
			dropped++
		}
	}
	return dropped
}

// Abort discards the segment's open page if it was never held.
func (w *PageWindow[T, C]) Abort(segment int) {
	w.mu.Lock()
	defer w.mu.Unlock()
	delete(w.open, segment)
}

// ReleaseReady sends every held page whose high canRelease accepts, holding
// the window's lock across send. Pages are released in no particular order
// (map iteration order): segments cover disjoint keys and each holds at most
// one page, so no two released pages carry the same key. A send error stops
// the pass and is returned, and the failing page stays held.
func (w *PageWindow[T, C]) ReleaseReady(canRelease func(high time.Time) bool, send func(ReleasedPage[T, C]) error) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	for seg, p := range w.held {
		if !canRelease(p.high) {
			continue
		}
		if err := send(ReleasedPage[T, C]{Segment: seg, Items: p.liveItems(), Cursor: p.cursor, High: p.high}); err != nil {
			return err
		}
		delete(w.held, seg)
		close(p.released)
	}
	return nil
}

// HeldItems counts live items across held pages.
func (w *PageWindow[T, C]) HeldItems() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	n := 0
	for _, p := range w.held {
		n += p.items.Len()
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
	for _, p := range w.held {
		if !found || p.high.Before(oldest) {
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
	for _, p := range w.held {
		close(p.released)
	}
	clear(w.held)
	clear(w.open)
}
