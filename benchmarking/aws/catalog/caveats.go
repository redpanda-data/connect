// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package catalog

// Caveat strings carried by plans, entries and recommendations.
const (
	CaveatSourceApproximated    = "source_approximated"
	CaveatSinkNotMeasured       = "sink_not_measured"
	CaveatExternalStubbed       = "external_calls_stubbed"
	CaveatSnapshotNotMeasured   = "snapshot_not_measured"
	CaveatUnreleasedBuild       = "unreleased_build"
	CaveatMemoryNotLimited      = "memory_not_limited"
	CaveatSchemaEvolutionForced = "schema_evolution_forced"
	CaveatSinkUnverified        = "sink_unverified"
	CaveatSinkRetried           = "sink_retried"
	// CaveatSourceRateLimited is declared in sizing.go.
)

// KnownCaveats maps every caveat string to a one-line meaning, so callers
// (the Bench Bot agent) can enumerate and explain them.
var KnownCaveats = map[string]string{
	CaveatSourceApproximated:    "the input has no bench stack; the bench read a Redpanda topic in its place",
	CaveatSinkNotMeasured:       "the output has no bench sink; messages were dropped, so sink throughput is not measured",
	CaveatExternalStubbed:       "processors that call external services were replaced with stubs",
	CaveatSnapshotNotMeasured:   "the CDC initial snapshot was turned off; only streaming changes were measured",
	CaveatUnreleasedBuild:       "measured on an unreleased build of main; a release may perform differently",
	CaveatMemoryNotLimited:      "points ran without a hard memory cap, so the memory bound is not measured",
	CaveatSourceRateLimited:     "the source (the bench producer, or the CDC workload's offered rate), not the pipeline, capped a point; its capacity is a lower bound",
	CaveatSchemaEvolutionForced: "the bench turned on sink schema evolution or table creation the customer config left off",
	CaveatSinkUnverified:        "at least one point could not check what the sink received (no sink count and no output_sent metric)",
	CaveatSinkRetried:           "at least one point had write errors on at most 1% of batches sent, so the sink retried writes; capacity includes the retries",
}
