// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License included
// in the licenses/BSL.md file.

package main

// sinkSpec captures the per-connector wiring for a sink bench, analogous to
// engineSpec for sources. Add a sink connector by adding one entry to
// sinkSpecs; touch no switch statements.
type sinkSpec struct {
	// OutputComponent is the Redpanda Connect output component key
	// (e.g. "iceberg") placed under pipeline.output.
	OutputComponent string
	// Namespace is the Iceberg namespace (Glue database) both engines write to.
	Namespace string
}

var sinkSpecs = map[string]sinkSpec{
	"iceberg": {
		OutputComponent: "iceberg",
		Namespace:       "bench",
	},
}

func sinkSpecFor(connector string) (sinkSpec, bool) {
	sp, ok := sinkSpecs[connector]
	return sp, ok
}
