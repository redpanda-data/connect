// Copyright 2025 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/v4/blob/main/licenses/rcl.md

package snapshot

import (
	"fmt"
	"strconv"
	"strings"
)

// Watermark is a simplified view of a Postgres txid_current_snapshot()
// result, keeping only the xmin/xmax bounds needed to reconcile the
// incremental snapshot window against the concurrently streamed
// transactions.
type Watermark struct {
	Xmin uint64
	Xmax uint64
}

// ParseSnapshot parses Postgres's txid_current_snapshot() text
// representation, e.g. "100:104:101,103" (xmin:xmax:xip_list, where the
// xip_list may be empty, e.g. "100:104:").
//
// The xip list (in-progress transaction ids at the time the snapshot was
// taken) is parsed only for validation of the input format. It is not
// retained: this package's window-open/close reconciliation only needs the
// xmin/xmax bounds, since it treats any transaction id in [xmin, xmax] that
// isn't explicitly reconciled by the concurrent replication stream as
// "already accounted for" rather than tracking individual in-progress ids.
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
