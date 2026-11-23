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
	"slices"

	"github.com/redpanda-data/connect/benchmarking/aws/catalog"
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

// Caveat strings attached to plans; catalog.KnownCaveats lists them all with
// their meanings.
const (
	CaveatSourceApproximated = catalog.CaveatSourceApproximated
	CaveatSinkNotMeasured    = catalog.CaveatSinkNotMeasured
	CaveatExternalStubbed    = catalog.CaveatExternalStubbed
	CaveatSnapshotNotMeasure = catalog.CaveatSnapshotNotMeasured
	CaveatUnreleasedBuild    = catalog.CaveatUnreleasedBuild
	CaveatMemoryNotLimited   = catalog.CaveatMemoryNotLimited
	CaveatSchemaEvoForced    = catalog.CaveatSchemaEvolutionForced
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

// classifySource returns the source kind for an input. Wrapper inputs
// (broker, sequence, read_until) are classified by their children: any CDC
// child without a bench stack makes the plan not runnable, children of one
// real kind give that kind, and a mix gives approximated. When not runnable,
// ok is false and reason is set.
func classifySource(in map[string]any) (kind SourceKind, reason string, ok bool, err error) {
	var kinds []SourceKind
	cdc := 0
	err = inputLeaves(in, func(_ map[string]any, name string) error {
		if catalog.ModeFor(name) == "cdc" {
			cdc++
		}
		if k, found := RealSources[name]; found {
			kinds = append(kinds, k)
			return nil
		}
		if catalog.ModeFor(name) == "cdc" {
			if reason == "" {
				reason = fmt.Sprintf("no bench stack for %s yet", name)
			}
			return nil
		}
		kinds = append(kinds, SourceApproximated)
		return nil
	})
	if err != nil {
		return "", "", false, err
	}
	if reason != "" {
		return "", reason, false, nil
	}
	if cdc > 1 {
		return "", "multiple CDC inputs in one pipeline not supported yet", false, nil
	}
	if len(kinds) == 0 || slices.Contains(kinds, SourceApproximated) || slices.ContainsFunc(kinds, func(k SourceKind) bool { return k != kinds[0] }) {
		return SourceApproximated, "", true, nil
	}
	return kinds[0], "", true, nil
}

func classifySink(output string) SinkKind {
	if k, found := RealSinks[output]; found {
		return k
	}
	return SinkDrop
}
