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

// Watermark holds the bounds of a Postgres snapshot, which the coordinator
// compares against streamed transactions. Xmin is the oldest transaction in
// flight, Xmax the first id not yet assigned.
type Watermark struct {
	Xmin uint64
	Xmax uint64
}

// OpensAt reports whether xid started at or after this watermark: at or
// above Xmin, the stream has passed everything the watermark could not see.
func (w Watermark) OpensAt(xid uint32) bool {
	return normalizeXID(xid, w.Xmin) >= w.Xmin
}

// ClosesAt reports whether xid follows every transaction in flight here:
// anything above Xmax started later.
func (w Watermark) ClosesAt(xid uint32) bool {
	return normalizeXID(xid, w.Xmax) > w.Xmax
}

// Quiesced reports whether nothing was in flight: equal bounds leave no id
// between them to be running.
func (w Watermark) Quiesced() bool {
	return w.Xmin == w.Xmax
}

const (
	xidEpoch     = 1 << 32
	xidHalfEpoch = 1 << 31
)

// normalizeXID widens a raw 32-bit transaction id into the epoch of ref, so
// the two can be compared. It picks the epoch that puts xid within half an
// epoch of ref, which is the nearest interpretation and the only one a
// running system can reach.
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

// ParseSnapshot reads a snapshot's text form, xmin:xmax:xip_list, as in
// "100:104:101,103". The xip list may be empty, and is validated then
// discarded: the window comparisons need only the bounds.
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
