// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License included
// in the licenses/BSL.md file.

package main

import "io"

// parseBrokerFrames splits a redpanda-<vcpu>.txt dump into the same
// frame shape that prom.go's parseSnapshots produces. We delegate to
// the existing private helper so a future schema change in the framing
// (timestamp marker format, etc.) only has to be made in one place.
//
// Returns (frames, nil) on success. The signature returns an error for
// forward-compatibility — if future framing changes need to surface
// I/O errors, we can fix that here without rippling through callers.
func parseBrokerFrames(r io.Reader) ([]promSnapshot, error) {
	return parseSnapshots(r), nil
}
