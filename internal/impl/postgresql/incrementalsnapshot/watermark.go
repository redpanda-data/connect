// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/v4/blob/main/licenses/rcl.md

package incrementalsnapshot

import (
	"fmt"
	"math"
	"strconv"
	"strings"
)

// Watermark is a simplified view of a Postgres txid_current_snapshot()
// result, keeping only the xmin/xmax bounds needed to reconcile the
// incremental snapshot window against the concurrently streamed
// transactions.
//
// It satisfies the shared package's Watermark[uint32] constraint (checked
// where CoordinatorConfig is instantiated), the position type being a raw
// transaction id as pgoutput reports it on a BEGIN message.
// The bounds here are epoch-extended 64-bit values, so every comparison
// goes through normalizeXID -- see the note there.
type Watermark struct {
	Xmin uint64
	Xmax uint64
}

// OpensAt reports whether xid started at or after this watermark was taken.
// Xmin is the oldest transaction still in flight at that point, so any xid
// at or above it began late enough that the stream is now past everything
// this watermark could not see.
func (w Watermark) OpensAt(xid uint32) bool {
	return normalizeXID(xid, w.Xmin) >= w.Xmin
}

// ClosesAt reports whether xid began after every transaction that was in
// flight when this watermark was taken. Xmax is the first id not yet
// assigned at that point, so anything strictly above it started later.
func (w Watermark) ClosesAt(xid uint32) bool {
	return normalizeXID(xid, w.Xmax) > w.Xmax
}

// Quiesced reports whether no transaction was in flight when this watermark
// was taken. Xmin is the oldest still running and Xmax the first not yet
// assigned, so their being equal means nothing was running at all.
func (w Watermark) Quiesced() bool {
	return w.Xmin == w.Xmax
}

const (
	xidEpoch     = 1 << 32
	xidHalfEpoch = 1 << 31
)

// normalizeXID lifts a raw 32-bit WAL xid into ref's epoch so the two are
// comparable.
//
// txid_current_snapshot() reports xmin/xmax epoch-extended to 64 bits, but
// pgoutput's BEGIN message carries only the low 32 bits, which wrap roughly
// every 4.3 billion transactions. Comparing the two directly means that on
// any cluster past its first wrap every watermark bound exceeds every
// possible xid, so the snapshot window never opens and the backfill stalls
// silently.
//
// A watermark is read either side of a chunk fetch and compared against
// commits arriving moments later, so the true xid is always far nearer to
// ref than half an epoch. Splicing ref's epoch onto xid therefore lands
// within one epoch of correct, and whichever neighbouring epoch puts the
// result inside that half-epoch window is the right one.
func normalizeXID(xid uint32, ref uint64) uint64 {
	full := (ref & ^uint64(math.MaxUint32)) | uint64(xid)
	switch {
	case full > ref && full-ref > xidHalfEpoch && full >= xidEpoch:
		// xid wrapped ahead of ref: it belongs to the preceding epoch.
		// Guarded against underflow, since epoch 0 has no predecessor.
		return full - xidEpoch
	case ref > full && ref-full > xidHalfEpoch:
		// ref wrapped ahead of xid: xid belongs to the following epoch.
		return full + xidEpoch
	default:
		return full
	}
}

// ParseSnapshot parses a Postgres snapshot's text form, e.g.
// "100:104:101,103" (xmin:xmax:xip_list; xip_list may be empty). Both
// pg_current_snapshot() (pg_snapshot, PG 13+) and the deprecated
// txid_current_snapshot() (txid_snapshot) render identically and carry
// epoch-extended 64-bit ids, so this handles either.
//
// The xip list is validated but not retained: window open/close
// reconciliation only needs the xmin/xmax bounds.
func ParseSnapshot(raw string) (Watermark, error) {
	parts := strings.Split(raw, ":")
	const expectedParts = 3
	if len(parts) != expectedParts {
		return Watermark{}, fmt.Errorf("invalid snapshot format %q: expected 3 colon-separated parts, got %d", raw, len(parts))
	}

	xmin, err := strconv.ParseUint(parts[0], 10, 64)
	if err != nil {
		return Watermark{}, fmt.Errorf("invalid snapshot format %q: xmin %q is not a valid uint64: %w", raw, parts[0], err)
	}

	xmax, err := strconv.ParseUint(parts[1], 10, 64)
	if err != nil {
		return Watermark{}, fmt.Errorf("invalid snapshot format %q: xmax %q is not a valid uint64: %w", raw, parts[1], err)
	}

	if xip := parts[2]; xip != "" {
		for id := range strings.SplitSeq(xip, ",") {
			if _, err := strconv.ParseUint(id, 10, 64); err != nil {
				return Watermark{}, fmt.Errorf("invalid snapshot format %q: xip entry %q is not a valid uint64: %w", raw, id, err)
			}
		}
	}

	return Watermark{Xmin: xmin, Xmax: xmax}, nil
}
