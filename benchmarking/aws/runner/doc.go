// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

// Command runner provisions AWS infrastructure, runs a Connect benchmark or
// soak sweep across vCPU points, and renders the results.
//
// This is a scope-reduced tree: source-direction, Connect-engine-only,
// postgres_cdc-only. It ships the soak testing pipeline (CON-179 R6) and the
// postgres_cdc bench sweep. Three seams return with their own future PRs:
//   - direction: sink (Iceberg sink topology, per-topic/per-stream naming)
//     returns with the iceberg-sink stack PR — see topologyFor in
//     topology.go for the validation error.
//   - The Kafka Connect / Debezium comparison (a second engine swept
//     alongside Connect, cross-engine divergence detection) returns with
//     the kafka-connect bench PR.
//   - mysql_cdc, oracledb_cdc, microsoft_sql_server_cdc, mongodb_cdc, and
//     aws_dynamodb_cdc each return with their own stack PR — see
//     engineSpecs in scenario.go.
//
// It is a single flat package by design (Go favours this for a CLI of this
// size). The files group by concern as follows — start at main.go and follow
// the phase it drives into the relevant group.
//
// Entry point & orchestration:
//   - main.go          CLI subcommands (bench, validate, down, cost-check,
//     summary) and the end-to-end run pipeline.
//   - matrix.go        the sweep: for each vCPU point, reset → launch
//     workload → run the bench → collect samples.
//
// Scenario & connector specs (the registries you extend per connector):
//   - scenario.go      the Scenario YAML schema, validation, and engineSpecs
//     (per-connector DSN / reset wiring for the Connect side).
//
// Topology (what infra a scenario needs and how seeding/workload are driven):
//   - topology.go        common topology interface.
//   - topology_source.go the CDC-source topology (postgres_cdc today; more
//     connectors return with their own stack PRs).
//
// Script rendering (shell run on the runner / load-gen hosts):
//   - scripts.go       seed, workload, and reset script rendering.
//   - render.go        Connect pipeline config rendering.
//
// Metrics, stats & analysis:
//   - prom.go            scrape Connect's :4195/metrics.
//   - brokermetrics.go   broker-side throughput (the canonical fairness metric).
//   - stats.go           percentiles / summary stats over samples.
//   - anomalies.go       flag dips / suspicious points before publishing.
//
// Infrastructure & artefacts:
//   - terraform.go     terraform apply/destroy of shared + stack.
//   - ssm.go           run scripts on EC2 via SSM.
//   - logfetcher.go    pull per-point logs from S3.
//   - cost.go          Cost Explorer spend reporting.
//
// Output:
//   - summary.go       regenerate the auto-managed section of SUMMARY.md.
//   - templates/       markdown templates for per-run results and the summary.
//
// Each <name>.go has a matching <name>_test.go; testdata/ holds scenario
// fixtures used by the validation tests.
package main
