// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License included
// in the licenses/BSL.md file.

package main

// kcDirection captures whether the connector reads from or writes to the
// shared Redpanda cluster. The matrix runner uses this to attribute the
// broker-side throughput metric to produce-side (sources) or consume-side
// (sinks) traffic. Plan 2 only ships sources.
type kcDirection int

const (
	kcSource kcDirection = iota
	kcSink
)

// kcConnectorSpec describes the Kafka Connect counterpart of a Redpanda
// Connect connector. Each entry pins the connector class, the JSON config
// template (which can reference scenario fields + TF outputs via Go
// text/template syntax), and any plugin globs that should exist on the
// runner host before the connector is submitted.
//
// To add a new connector to the comparison framework, add one entry to
// kcConnectorSpecs below. Touch no other files.
type kcConnectorSpec struct {
	Class           string
	PropsTemplate   string
	Direction       kcDirection
	RequiredPlugins []string
}

// kcConnectorSpecs is the registry of KC counterparts keyed by the Redpanda
// Connect connector name (the same key used in engineSpecs).
var kcConnectorSpecs = map[string]kcConnectorSpec{}

func kcConnectorSpecFor(connector string) (kcConnectorSpec, bool) {
	es, ok := kcConnectorSpecs[connector]
	return es, ok
}
