// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package catalog

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
)

// KeepStringKeys lists the keys whose string values are kept when
// fingerprinting. All other string values are blanked since they are
// identity, not performance.
var KeepStringKeys = []string{
	"mapping",             // Bloblang text changes processing cost.
	"check",               // Bloblang predicate changes processing cost.
	"compression",         // Compression algorithm changes CPU and throughput.
	"codec",               // Codec changes parsing cost.
	"scanner",             // Scanner changes parsing cost.
	"format",              // Output/serialisation format changes cost.
	"period",              // Batch flush period changes batching behaviour.
	"byte_size",           // Batch byte size changes batching behaviour.
	"max_in_flight",       // Concurrency knob.
	"checkpoint_limit",    // Concurrency knob.
	"stream_snapshot",     // Snapshot vs streaming is a different workload.
	"snapshot_batch_size", // Snapshot read size changes throughput.
	"mutation",            // Bloblang mutation text changes processing cost.
	"bloblang",            // Bloblang processor text changes processing cost.
	"request_map",         // branch request_map Bloblang changes processing cost.
	"result_map",          // branch result_map Bloblang changes processing cost.
	"query",               // jq query text changes processing cost.
	"code",                // javascript processor code changes processing cost.
}

// ignoredKeys are dropped entirely when fingerprinting: they identify a
// deployment rather than affect performance.
var ignoredKeys = []string{
	"label", "tls", "sasl", "auth", "credentials", "password", "user",
	"username", "dsn", "url", "urls", "addresses", "seed_brokers", "topic", "topics",
	"tables", "schema",
}

// Components returns component names for input, processors and output.
// Processors are the top-level names only (no recursion), ordered as
// input.processors, pipeline.processors, output.processors. A "processors"
// list inside the input or output map is not a component. Accepts a pipeline
// map with keys "input", optional "pipeline" ({"processors": [...]}), and
// "output".
func Components(pipeline map[string]any) (input string, processors []string, output string, err error) {
	inMap, err := sectionMap(pipeline, "input")
	if err != nil {
		return "", nil, "", err
	}
	outMap, err := sectionMap(pipeline, "output")
	if err != nil {
		return "", nil, "", err
	}

	input, inProcs, err := componentWithProcessors(inMap, "input")
	if err != nil {
		return "", nil, "", err
	}
	output, outProcs, err := componentWithProcessors(outMap, "output")
	if err != nil {
		return "", nil, "", err
	}

	var pipeProcs []any
	if raw, ok := pipeline["pipeline"]; ok && raw != nil {
		pm, ok := raw.(map[string]any)
		if !ok {
			return "", nil, "", errors.New("pipeline must be a map")
		}
		if pipeProcs, err = listField(pm, "processors", "pipeline.processors"); err != nil {
			return "", nil, "", err
		}
	}

	for _, group := range [][]any{inProcs, pipeProcs, outProcs} {
		for _, p := range group {
			name, err := processorName(p)
			if err != nil {
				return "", nil, "", err
			}
			processors = append(processors, name)
		}
	}
	return input, processors, output, nil
}

func sectionMap(pipeline map[string]any, key string) (map[string]any, error) {
	raw, ok := pipeline[key]
	if !ok {
		return nil, fmt.Errorf("pipeline has no %s", key)
	}
	m, ok := raw.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("%s must be a map", key)
	}
	return m, nil
}

func listField(m map[string]any, key, path string) ([]any, error) {
	raw, ok := m[key]
	if !ok || raw == nil {
		return nil, nil
	}
	l, ok := raw.([]any)
	if !ok {
		return nil, fmt.Errorf("%s must be a list", path)
	}
	return l, nil
}

// componentWithProcessors extracts the single component name from an input or
// output map and its nested processors list.
func componentWithProcessors(m map[string]any, section string) (string, []any, error) {
	procs, err := listField(m, "processors", section+".processors")
	if err != nil {
		return "", nil, err
	}
	var names []string
	for k := range m {
		if k == "processors" || k == "label" {
			continue
		}
		names = append(names, k)
	}
	if len(names) != 1 {
		slices.Sort(names)
		return "", nil, fmt.Errorf("%s must have exactly one component, found %v", section, names)
	}
	return names[0], procs, nil
}

func processorName(p any) (string, error) {
	m, ok := p.(map[string]any)
	if !ok {
		return "", fmt.Errorf("processor must be a map, got %T", p)
	}
	var names []string
	for k := range m {
		if k != "label" {
			names = append(names, k)
		}
	}
	if len(names) != 1 {
		slices.Sort(names)
		return "", fmt.Errorf("processor must have exactly one component, found %v", names)
	}
	return names[0], nil
}

// identitySections are the top-level config keys that make up a pipeline's
// catalogue identity. Every other key (logger, http, metrics, tracer,
// resources, ...) is ignored: it does not change throughput.
var identitySections = []string{"input", "buffer", "pipeline", "output"}

// ConfigIdentity is the catalogue identity of a whole Connect config: its
// components and the fingerprint of its input, buffer, pipeline and output.
// Plans and catalogue queries both use it, so a re-asked config matches the
// entry its plan recorded.
func ConfigIdentity(config map[string]any) (Identity, error) {
	sections := map[string]any{}
	for _, k := range identitySections {
		if v, ok := config[k]; ok {
			sections[k] = v
		}
	}
	input, processors, output, err := Components(sections)
	if err != nil {
		return Identity{}, fmt.Errorf("components: %w", err)
	}
	fp, err := Fingerprint(sections)
	if err != nil {
		return Identity{}, fmt.Errorf("fingerprint: %w", err)
	}
	return Identity{Fingerprint: fp, Input: input, Processors: processors, Output: output}, nil
}

// Fingerprint is the hex sha256 of the canonical JSON of the normalised
// pipeline. Callers must pass values decoded from JSON/YAML into
// map[string]any / []any, since typed maps and slices are not normalised.
func Fingerprint(pipeline map[string]any) (string, error) {
	b, err := json.Marshal(normalise("", pipeline))
	if err != nil {
		return "", fmt.Errorf("marshalling normalised pipeline: %w", err)
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:]), nil
}

// normalise drops identity keys and blanks strings that are not performance
// knobs. key is the nearest enclosing map key, so list elements inherit it.
func normalise(key string, v any) any {
	switch t := v.(type) {
	case map[string]any:
		out := make(map[string]any, len(t))
		for k, child := range t {
			if slices.Contains(ignoredKeys, k) {
				continue
			}
			out[k] = normalise(k, child)
		}
		return out
	case []any:
		out := make([]any, len(t))
		for i, child := range t {
			out[i] = normalise(key, child)
		}
		return out
	case string:
		if slices.Contains(KeepStringKeys, key) {
			return t
		}
		return ""
	default:
		return v
	}
}
