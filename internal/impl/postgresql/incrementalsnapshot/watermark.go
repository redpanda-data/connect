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

// Watermark holds the xmin/xmax bounds of a Postgres snapshot, which the
// coordinator compares against streamed transactions.
//
// It satisfies the shared package's Watermark[uint32] constraint, checked
// where CoordinatorConfig is instantiated. The position is a raw transaction
// id as pgoutput reports it on BEGIN, while the bounds here are
// epoch-extended, so every comparison goes through normalizeXID.
type Watermark struct {
	Xmin uint64
	Xmax uint64
}

// OpensAt reports whether xid started at or after this watermark. Xmin is
// the oldest transaction then in flight, so anything at or above it began
// late enough that the stream has passed everything this watermark could not
// see.
func (w Watermark) OpensAt(xid uint32) bool {
	return normalizeXID(xid, w.Xmin) >= w.Xmin
}

// ClosesAt reports whether xid follows every transaction in flight at this
// watermark. Xmax is the first id not yet assigned, so anything above it
// started later.
func (w Watermark) ClosesAt(xid uint32) bool {
	return normalizeXID(xid, w.Xmax) > w.Xmax
}

// Quiesced reports whether nothing was in flight. Xmin is the oldest
// transaction still running and Xmax the first id not yet assigned, so equal
// values mean nothing was running at all.
func (w Watermark) Quiesced() bool {
	return w.Xmin == w.Xmax
}

const (
	xidEpoch     = 1 << 32
	xidHalfEpoch = 1 << 31
)

// normalizeXID lifts a raw 32-bit WAL xid into ref's epoch so the two become
// comparable.
//
// Snapshot bounds are epoch-extended to 64 bits, but pgoutput's BEGIN carries
// only the low 32 bits, which wrap every ~4.3 billion transactions. Comparing
// them directly fails after the first wrap: every bound then exceeds every
// possible xid, so the window never opens and the snapshot stalls silently.
//
// Watermarks are read either side of a chunk and compared against commits
// arriving moments later, so the true xid is always within half an epoch of
// ref. Splicing ref's epoch onto xid therefore lands within one epoch, and
// whichever neighbouring epoch falls inside that half-epoch window is right.
func normalizeXID(xid uint32, ref uint64) uint64 {
	full := (ref & ^uint64(math.MaxUint32)) | uint64(xid)
	switch {
	case full > ref && full-ref > xidHalfEpoch && full >= xidEpoch:
		// xid wrapped ahead of ref, so it belongs to the previous epoch.
		// The last test guards underflow: epoch 0 has no predecessor.
		return full - xidEpoch
	case ref > full && ref-full > xidHalfEpoch:
		// ref wrapped ahead of xid, so xid belongs to the next epoch.
		return full + xidEpoch
	default:
		return full
	}
}

// ParseSnapshot reads a Postgres snapshot's text form, e.g. "100:104:101,103"
// (xmin:xmax:xip_list, where the xip list may be empty). Both
// pg_current_snapshot (PG 13+) and the obsolete txid_current_snapshot render
// this way with epoch-extended ids, so either result parses.
//
// The xip list is validated but discarded: the window comparisons need only
// the bounds.
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
