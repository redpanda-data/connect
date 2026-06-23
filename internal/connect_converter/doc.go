// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

// Package connectconverter turns a Kafka Connect connector config (REST-wrapped
// or flat JSON) into an equivalent Redpanda Connect pipeline YAML.
//
// # Where to start
//
// The single entry point is Convert(input []byte) (*Result, error) in
// engine_convert.go. Read that function first — it is the whole pipeline in ~40
// lines and links to everything else:
//
//	parse()          engine_parse.go    JSON  -> ConnectConfig{Name, Class, Props}
//	lookupConnector  registry.go        connector.class -> ConnectorMapper
//	  .Map()         conn_*.go          build the input/output component + Encode steps
//	mapConverters()  engine_convert.go  value.converter -> decode processors (conv_serializers.go)
//	mapSMTs()        engine_convert.go  transforms.*    -> Bloblang processors (smt_*.go)
//	comp.Encode      conn_s3/gcs.go     re-encode processors (run last, before output)
//	assemble()       engine_assemble.go stitch input / pipeline.processors / output
//	render()         engine_render.go   encode to YAML with inline # TODO comments
//
// Everything a mapper needs is threaded through *MapCtx (mapctx.go): it reads
// config props, marks them "consumed", and records Warnings. Any prop never
// consumed surfaces as an inline "# TODO: unmapped field" comment, so nothing is
// silently dropped.
//
// # File layout
//
//	engine_*.go    The conversion pipeline: convert (orchestration), parse,
//	               assemble, render. Start here.
//	registry.go    The three plugin registries (connectors, converters, SMTs)
//	               populated by each mapper's init(); plus the fallback stubs.
//	mapctx.go      MapCtx: per-conversion scratchpad (props, consumed, warnings).
//	types.go       ConnectConfig, Component, Result, Warning, ConverterRole.
//	yaml.go        Tiny yaml.Node builders (scalar/mapping/seq/kv/component).
//
//	conn_*.go      One file per connector family -> RPCN input/output:
//	               s3, gcs, bigquery, snowflake, iceberg (sinks); jdbc
//	               (source+sink); debezium (pg/mysql/sqlserver/oracle CDC);
//	               mirror + replicator (kafka->kafka, redpanda in+out).
//	conv_*.go      value.converter -> decode processors (schema_registry_decode,
//	               JSON envelope unwrap, no-op for already-structured data).
//	smt_*.go       One file per Single Message Transform family -> Bloblang.
//	helpers_*.go   Shared building blocks: helpers_field.go (batching, object
//	               paths/partitioners/encode, consume helpers) and helpers_smt.go
//	               (fieldPath quoting, predicate resolution, joda->go layout).
//
//	testdata/      Golden fixtures: <case>.input.json + <case>.expected.yaml.
//
// # How to debug
//
// Every *.go has a colocated *_test.go (Go requires same-package tests for the
// unexported API). Cross-cutting test files:
//
//	testsupport_test.go    Shared harness: the component blank-imports the
//	                       benthos linter needs + assertValidRPCN / gapConvert.
//	golden_test.go         End-to-end: runs every testdata case through Convert
//	                       and diffs against the golden YAML. THE place to start
//	                       when output changes. Regenerate with:
//	                           go test ./internal/connect_converter/ -run TestGolden -update
//	                       then review the diff.
//	gap_fixes_test.go      Per-connector correctness regressions (G1..G15).
//	decode_encode_test.go  Serialization round-trip (decode + re-encode).
//	smt_*_test.go          SMT behavior, including predicate gating and the
//	                       gap/edge-case suites.
//
// Debug flow for a wrong conversion:
//  1. Reproduce with a minimal JSON config (or add a testdata case).
//  2. assertValidRPCN(t, res.YAML) confirms the output lints against real RPCN
//     component schemas — not just that it is valid YAML.
//  3. Inspect res.Warnings and the inline "# TODO" comments: they pinpoint the
//     prop the converter could not fully map and which mapper handled it.
//  4. Trace from Convert() -> the relevant conn_*/conv_*/smt_* mapper.
package connectconverter
