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

// Watermark holds the xmin and xmax bounds of a Postgres snapshot. The
// coordinator uses these bounds to compare the snapshot window with the
// streamed transactions.
//
// Watermark satisfies the Watermark[uint32] constraint of the shared
// package. The compiler makes this check where the code declares
// CoordinatorConfig. The position type is a transaction id as pgoutput
// reports it in a BEGIN message.
//
// The bounds are 64-bit values that include an epoch. Therefore each
// comparison calls normalizeXID. Refer to that function.
type Watermark struct {
	Xmin uint64
	Xmax uint64
}

// OpensAt tells if xid started at the same time as this watermark or after
// it. Xmin is the oldest transaction that was in flight. An xid that is
// equal to xmin or larger started late enough. The stream has then passed
// all transactions that this watermark could not see.
func (w Watermark) OpensAt(xid uint32) bool {
	return normalizeXID(xid, w.Xmin) >= w.Xmin
}

// ClosesAt tells if xid started after all transactions that were in flight
// at this watermark. Xmax is the first id that Postgres had not given out.
// Therefore each larger xid started later.
func (w Watermark) ClosesAt(xid uint32) bool {
	return normalizeXID(xid, w.Xmax) > w.Xmax
}

// Quiesced tells if no transaction was in flight at this watermark. Xmin is
// the oldest transaction that was still running. Xmax is the first id that
// Postgres had not given out. Equal values therefore show that no
// transaction was running.
func (w Watermark) Quiesced() bool {
	return w.Xmin == w.Xmax
}

const (
	xidEpoch     = 1 << 32
	xidHalfEpoch = 1 << 31
)

// normalizeXID adds the epoch of ref to a 32-bit WAL xid. The two values are
// then comparable.
//
// A Postgres snapshot gives xmin and xmax as 64-bit values that include an
// epoch. A pgoutput BEGIN message gives only the low 32 bits. These 32 bits
// return to zero after approximately 4.3 billion transactions.
//
// A direct comparison of the two values fails after the first return to
// zero. Each watermark bound is then larger than each possible xid. The
// window never opens and the snapshot stops without a message.
//
// The coordinator reads a watermark before and after each chunk read. It
// compares the watermarks with commits that arrive a short time later.
// Therefore the true xid is always nearer to ref than half an epoch. This
// function adds the epoch of ref to xid. It then selects the epoch that puts
// the result within half an epoch of ref.
func normalizeXID(xid uint32, ref uint64) uint64 {
	full := (ref & ^uint64(math.MaxUint32)) | uint64(xid)
	switch {
	case full > ref && full-ref > xidHalfEpoch && full >= xidEpoch:
		// xid returned to zero after ref, so xid is in the epoch before
		// ref. The last test prevents underflow, because epoch 0 has no
		// earlier epoch.
		return full - xidEpoch
	case ref > full && ref-full > xidHalfEpoch:
		// ref returned to zero after xid, so xid is in the epoch after
		// ref.
		return full + xidEpoch
	default:
		return full
	}
}

// ParseSnapshot reads the text form of a Postgres snapshot, for example
// "100:104:101,103". The parts are xmin, xmax and the xip list, and the xip
// list can be empty.
//
// pg_current_snapshot on PostgreSQL 13 and later and the obsolete
// txid_current_snapshot both use this same form. Both give 64-bit ids that
// include an epoch. Therefore this function reads the result of either
// function.
//
// The function checks the xip list but does not keep it. The window
// comparisons need the xmin and xmax bounds only.
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
