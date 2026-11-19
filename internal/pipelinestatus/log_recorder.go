// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package pipelinestatus

import (
	"cmp"
	"context"
	"errors"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redpanda-data/benthos/v4/public/utils/redact"
)

const (
	logAttrStream = "stream"
	logAttrLabel  = "label"
	logAttrPath   = "path"
	logAttrError  = "error"
	logAttrErr    = "err"

	// componentPathRoot is the prefix the component manager gives every path
	// attribute, which tells it apart from a component's own `path` attribute.
	componentPathRoot = "root"

	// maxRings bounds the number of components with a ring. Components come
	// and go in streams mode and under config reloads, so without a bound the
	// rings of components that no longer exist would accumulate for the life
	// of the process. When the bound is reached the ring written to least
	// recently is dropped.
	maxRings = 1024
)

// recentErrors keeps a bounded ring of log entries per component.
type recentErrors struct {
	perComponent int

	mut   sync.Mutex
	seq   uint64
	rings map[string]*errorRing
}

func newRecentErrors(perComponent int) *recentErrors {
	return &recentErrors{
		perComponent: perComponent,
		rings:        map[string]*errorRing{},
	}
}

type errorRing struct {
	entries []logEntry
	next    int
	full    bool

	// last is the seq of the most recent entry, used to pick a ring to evict.
	last uint64
}

func (r *recentErrors) add(e logEntry) {
	key := e.Stream + "\x00" + e.Label + "\x00" + e.Path

	r.mut.Lock()
	defer r.mut.Unlock()

	ring := r.rings[key]
	if ring == nil {
		if len(r.rings) >= maxRings {
			r.evictStalest()
		}
		ring = &errorRing{entries: make([]logEntry, r.perComponent)}
		r.rings[key] = ring
	}
	r.seq++
	e.seq = r.seq
	ring.last = e.seq
	ring.entries[ring.next] = e
	ring.next = (ring.next + 1) % r.perComponent
	if ring.next == 0 {
		ring.full = true
	}
}

// evictStalest drops the ring whose most recent entry is the oldest. The
// caller holds the lock.
func (r *recentErrors) evictStalest() {
	var (
		stalestKey string
		stalest    uint64
		found      bool
	)
	for key, ring := range r.rings {
		if !found || ring.last < stalest {
			stalestKey, stalest, found = key, ring.last, true
		}
	}
	if found {
		delete(r.rings, stalestKey)
	}
}

// entries returns every retained entry in the order they were recorded.
func (r *recentErrors) entries() []logEntry {
	r.mut.Lock()
	defer r.mut.Unlock()

	out := make([]logEntry, 0)
	for _, ring := range r.rings {
		out = append(out, ring.ordered()...)
	}
	slices.SortFunc(out, func(a, b logEntry) int {
		return cmp.Compare(a.seq, b.seq)
	})
	return out
}

func (ring *errorRing) ordered() []logEntry {
	if !ring.full {
		return slices.Clone(ring.entries[:ring.next])
	}
	out := make([]logEntry, 0, len(ring.entries))
	out = append(out, ring.entries[ring.next:]...)
	out = append(out, ring.entries[:ring.next]...)
	return out
}

//------------------------------------------------------------------------------

// errorRecorder is a slog handler that records ERROR level entries into a
// recentErrors, attributed by the stream, label and path attributes the
// component manager attaches to each component's logger.
//
// The manager builds a component's path incrementally, so the last `path`
// attribute attached with WithAttrs is the most specific, and every path it
// attaches starts with `root`. A component's own `path` attribute, whether on
// the record or through With, such as a file path, has no such prefix and
// never changes which component an entry is filed under.
type errorRecorder struct {
	enabled *atomic.Bool
	recent  *recentErrors
	attrs   []slog.Attr
}

func (h *errorRecorder) Enabled(_ context.Context, level slog.Level) bool {
	return level >= slog.LevelError && h.enabled.Load()
}

func (h *errorRecorder) Handle(ctx context.Context, r slog.Record) error {
	if !h.Enabled(ctx, r.Level) {
		return nil
	}

	var stream, label, path, errStr string
	for _, a := range h.attrs {
		switch a.Key {
		case logAttrStream:
			stream = a.Value.String()
		case logAttrLabel:
			label = a.Value.String()
		case logAttrPath:
			if v := a.Value.String(); isComponentPath(v) {
				path = v
			}
		case logAttrError, logAttrErr:
			errStr = a.Value.String()
		}
	}
	// An error attached with the handler's attributes counts as well as one on
	// the record: the benthos service.Logger.With goes through WithAttrs, and
	// `logger.With("error", err).Error("...")` is how components report one.
	r.Attrs(func(a slog.Attr) bool {
		if a.Key == logAttrError || a.Key == logAttrErr {
			errStr = a.Value.String()
		}
		return true
	})

	// Older printf-style log calls in benthos end their message with a
	// newline, which has no place in a JSON field.
	// Components often log both the formatted error in the message and the
	// error as an attribute, so the attribute is only appended when the
	// message doesn't already carry it.
	msg := strings.TrimSpace(r.Message)
	if errStr = strings.TrimSpace(errStr); errStr != "" && !strings.Contains(msg, errStr) {
		msg += ": " + errStr
	}

	ts := r.Time
	if ts.IsZero() {
		ts = time.Now()
	}

	h.recent.add(logEntry{
		Stream:    stream,
		Label:     label,
		Path:      componentPath(path),
		Message:   redactText(msg),
		Timestamp: ts.Unix(),
	})
	return nil
}

func (h *errorRecorder) WithAttrs(attrs []slog.Attr) slog.Handler {
	newH := *h
	newH.attrs = make([]slog.Attr, 0, len(h.attrs)+len(attrs))
	newH.attrs = append(newH.attrs, h.attrs...)
	newH.attrs = append(newH.attrs, attrs...)
	return &newH
}

func (h *errorRecorder) WithGroup(string) slog.Handler {
	return h
}

// isComponentPath reports whether a path attribute was attached by the
// component manager, which roots every path it attaches at `root`.
func isComponentPath(p string) bool {
	return p == componentPathRoot || strings.HasPrefix(p, componentPathRoot+".")
}

// componentPath converts the path attribute of a component logger, which is
// rooted at `root`, to the dot path form used by connection statuses.
func componentPath(p string) string {
	if p == componentPathRoot {
		return ""
	}
	return strings.TrimPrefix(p, componentPathRoot+".")
}

// redactText removes credentials from a free text message, such as a URL or
// DSN quoted in a log line.
func redactText(s string) string {
	if s == "" {
		return s
	}
	return redact.Error(errors.New(s)).Error()
}
