// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License included
// in the licenses/BSL.md file.

package main

import "sort"

// sinkSpec captures the per-connector wiring for a sink bench, analogous to
// engineSpec for sources. Add a sink connector by adding one entry to
// sinkSpecs; touch no switch statements. The generic sink shell
// (topology_sink.go) owns the redpanda input, topic seeding, and the metric
// frame format; the hooks below own everything connector-specific.
type sinkSpec struct {
	// OutputComponent is the Redpanda Connect output component key
	// (e.g. "iceberg", "snowflake_streaming") placed under pipeline.output.
	OutputComponent string
	// HelperBinary names the connector's table helper under seeders/<name>,
	// cross-built and staged to /opt/bench/<name> for ResetScript and
	// SidecarSetup to invoke. Empty when the connector needs none.
	HelperBinary string
	// ArtifactPrefix prefixes the per-point metric dump basename
	// (e.g. "iceberg" -> iceberg-4-connect.txt).
	ArtifactPrefix string
	// Namespace is the Iceberg namespace (Glue database) both engines write
	// to. Iceberg-only.
	Namespace string
	// DecorateOutput fills the connector-specific fields of the scenario's
	// pipeline.output.<OutputComponent> map: endpoints via ${TF_OUTPUT}
	// placeholders, table names via BenchNames literals.
	DecorateOutput func(s *Scenario, n BenchNames, cfg map[string]any)
	// ResetScript renders the between-points reset: drop/recreate the sink
	// tables and rewind consumer groups so every sweep point starts at zero.
	ResetScript func(s *Scenario, outs map[string]string, n BenchNames) string
	// SidecarSetup renders the background poller that samples the sink's
	// committed rows/bytes into $RP in ParseIcebergSeries frame format. It
	// must define $RP as /tmp/<artifact> and end with RP_SCRAPER=$!.
	SidecarSetup func(args MetricSidecarArgs, artifact string) string
	// KCConfig renders the Kafka Connect counterpart connector. nil means the
	// sink has no KC counterpart wired (Connect-only benches): runs with
	// --engines including kafka_connect fail up front.
	KCConfig func(s *Scenario, outs map[string]string, n BenchNames) (KCRenderResult, error)
}

var sinkSpecs = map[string]sinkSpec{
	"iceberg":   icebergSinkSpec,
	"snowflake": snowflakeSinkSpec,
}

func sinkSpecFor(connector string) (sinkSpec, bool) {
	sp, ok := sinkSpecs[connector]
	return sp, ok
}

// sinkHelperBinaries is every registered helper binary name, sorted for
// deterministic script rendering. stageArtefacts downloads each best-effort;
// only the connector-under-test's helper is actually staged in S3.
func sinkHelperBinaries() []string {
	var out []string
	for _, sp := range sinkSpecs {
		if sp.HelperBinary != "" {
			out = append(out, sp.HelperBinary)
		}
	}
	sort.Strings(out)
	return out
}
