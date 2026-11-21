// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package catalog

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestEstimateRun(t *testing.T) {
	e, err := EstimateRun("postgres", "snowflake", "MEDIUM")
	require.NoError(t, err)
	require.InDelta(t, 4.0, e.SaaSUSD, 1e-9)
	require.InDelta(t, 2.5, e.AWSUSD, 1e-9)
	require.Equal(t, 25, e.Minutes)

	e, err = EstimateRun("postgres", "snowflake", "XLARGE")
	require.NoError(t, err)
	require.InDelta(t, 16.0, e.SaaSUSD, 1e-9)

	e, err = EstimateRun("postgres", "snowflake", "XSMALL")
	require.NoError(t, err)
	require.InDelta(t, 1.0, e.SaaSUSD, 1e-9)

	e, err = EstimateRun("none", "bigquery", "")
	require.NoError(t, err)
	require.InDelta(t, 0.5, e.SaaSUSD, 1e-9)
	require.Positive(t, e.Minutes)

	e, err = EstimateRun("mysql", "drop", "")
	require.NoError(t, err)
	require.Zero(t, e.SaaSUSD)
	require.Positive(t, e.AWSUSD)

	for _, c := range []struct{ stack, sink, size string }{
		{"oracle", "drop", ""},
		{"postgres", "kinesis", ""},
		{"postgres", "snowflake", "XXLARGE"},
		{"postgres", "snowflake", ""},
		{"postgres", "s3", "LARGE"},
	} {
		_, err := EstimateRun(c.stack, c.sink, c.size)
		require.Error(t, err, "%+v", c)
	}
}

func TestEstimatesJSONCoversEveryStackAndSink(t *testing.T) {
	cfg, err := loadEstimates(defaultEstimatesJSON)
	require.NoError(t, err)
	for _, s := range []string{"postgres", "mysql", "none"} {
		require.Contains(t, cfg.Stacks, s)
	}
	for _, s := range []string{"snowflake", "bigquery", "iceberg", "s3", "redpanda", "drop"} {
		require.Contains(t, cfg.Sinks, s)
	}
}

func TestKnownCaveats(t *testing.T) {
	for _, c := range []string{
		"source_approximated", "sink_not_measured", "external_calls_stubbed",
		"snapshot_not_measured", "unreleased_build", "memory_not_limited",
		"source_rate_limited", "schema_evolution_forced",
	} {
		require.NotEmpty(t, KnownCaveats[c], c)
	}
	require.Len(t, KnownCaveats, 8)
}
