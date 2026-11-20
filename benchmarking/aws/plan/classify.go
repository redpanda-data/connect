// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package plan

import (
	"fmt"

	"github.com/redpanda-data/connect/v4/benchmarking/aws/catalog"
)

// SourceKind says which bench source stack a plan uses.
type SourceKind string

// Source kinds.
const (
	SourcePostgres     SourceKind = "postgres"
	SourceMySQL        SourceKind = "mysql"
	SourceRedpanda     SourceKind = "redpanda"
	SourceApproximated SourceKind = "approximated"
)

// SinkKind says which bench sink a plan writes to.
type SinkKind string

// Sink kinds.
const (
	SinkRedpanda  SinkKind = "redpanda"
	SinkS3        SinkKind = "s3"
	SinkIceberg   SinkKind = "iceberg"
	SinkSnowflake SinkKind = "snowflake"
	SinkBigQuery  SinkKind = "bigquery"
	SinkDrop      SinkKind = "drop"
)

// Caveat strings attached to plans.
const (
	CaveatSourceApproximated = "source_approximated"
	CaveatSinkNotMeasured    = "sink_not_measured"
	CaveatExternalStubbed    = "external_calls_stubbed"
	CaveatSnapshotNotMeasure = "snapshot_not_measured"
	CaveatUnreleasedBuild    = "unreleased_build"
	CaveatMemoryNotLimited   = "memory_not_limited"
)

// RealSources maps customer input components to the bench source stack that
// measures them for real.
var RealSources = map[string]SourceKind{
	"postgres_cdc":    SourcePostgres,
	"mysql_cdc":       SourceMySQL,
	"kafka_franz":     SourceRedpanda,
	"kafka":           SourceRedpanda,
	"redpanda":        SourceRedpanda,
	"redpanda_common": SourceRedpanda,
}

// RealSinks maps customer output components to the bench sink that measures
// them for real.
var RealSinks = map[string]SinkKind{
	"redpanda":            SinkRedpanda,
	"kafka_franz":         SinkRedpanda,
	"kafka":               SinkRedpanda,
	"redpanda_common":     SinkRedpanda,
	"aws_s3":              SinkS3,
	"iceberg":             SinkIceberg,
	"snowflake_streaming": SinkSnowflake,
	"snowflake_put":       SinkSnowflake,
	"gcp_bigquery":        SinkBigQuery,
}

// classifySource returns the source kind for an input component. A CDC input
// without a bench stack is not runnable, in which case ok is false and reason
// is set.
func classifySource(input string) (kind SourceKind, reason string, ok bool) {
	if k, found := RealSources[input]; found {
		return k, "", true
	}
	if catalog.ModeFor(input) == "cdc" {
		return "", fmt.Sprintf("no bench stack for %s yet", input), false
	}
	return SourceApproximated, "", true
}

func classifySink(output string) SinkKind {
	if k, found := RealSinks[output]; found {
		return k
	}
	return SinkDrop
}
