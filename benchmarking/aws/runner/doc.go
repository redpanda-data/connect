// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License included
// in the licenses/BSL.md file.

// Command runner provisions AWS infrastructure, runs a Connect-vs-Kafka-Connect
// benchmark sweep across vCPU points, and renders the results.
//
// It is a single flat package by design (Go favours this for a CLI of this
// size). The files group by concern as follows — start at main.go and follow
// the phase it drives into the relevant group.
//
// Entry point & orchestration:
//   - main.go          CLI subcommands (bench, validate, down, cost-check,
//                      summary) and the end-to-end run pipeline.
//   - matrix.go        the sweep: for each vCPU point and engine, reset →
//                      launch workload → run the bench → collect samples.
//
// Scenario & connector specs (the registries you extend per connector):
//   - scenario.go      the Scenario YAML schema, validation, and engineSpecs
//                      (per-connector DSN / reset wiring for the Connect side).
//   - kcconnectors.go  kcConnectorSpecs (per-connector Kafka Connect / Debezium
//                      connector config + required plugins).
//   - sinkspecs.go     sink-topology specs (e.g. iceberg).
//
// Topology (what infra a scenario needs and how seeding/workload are driven):
//   - topology.go        common topology interface.
//   - topology_source.go CDC-source topologies (postgres/mysql/oracle/dynamodb).
//   - topology_sink.go   sink topologies (iceberg).
//
// Script rendering (shell run on the runner / load-gen hosts):
//   - scripts.go       seed, workload, and reset script rendering.
//   - render.go        Connect pipeline config rendering.
//   - kcscript.go      Kafka Connect bench script rendering.
//   - kcrest.go        Kafka Connect REST API (deploy/delete connectors).
//
// Metrics, stats & analysis:
//   - prom.go            scrape Connect's :4195/metrics.
//   - brokermetrics.go   broker-side throughput (the canonical fairness metric).
//   - icebergmetrics.go  iceberg sink-specific metrics.
//   - stats.go           percentiles / summary stats over samples.
//   - anomalies.go       flag dips / suspicious points before publishing.
//   - crossengine.go     Connect-vs-KC divergence detection.
//
// Infrastructure & artefacts:
//   - terraform.go     terraform apply/destroy of shared + stack.
//   - ssm.go           run scripts on EC2 via SSM.
//   - logfetcher.go    pull per-engine logs from S3.
//   - cost.go          Cost Explorer spend reporting.
//
// Output:
//   - summary.go       regenerate the auto-managed section of SUMMARY.md.
//   - templates/       markdown templates for per-run results and the summary.
//
// Each <name>.go has a matching <name>_test.go; testdata/ holds scenario
// fixtures used by the validation tests.
package main
