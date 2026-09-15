// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package connectconverter

import (
	"fmt"
	"strings"

	"gopkg.in/yaml.v3"
)

// assemble builds the RPCN stream node tree. A nil Input/Output side becomes a
// commented TODO stub. Unmapped field TODOs are attached to the primary
// component node (output if present, else input).
func assemble(cfg ConnectConfig, comp Component, procs []*yaml.Node, unmapped []string) *yaml.Node {
	root := mapping()
	root.HeadComment = fmt.Sprintf(
		"Converted from Kafka Connect connector %q (class=%s). Review # TODO markers.",
		cfg.Name, cfg.Class,
	)

	input := comp.Input
	if input == nil {
		stub := mapping()
		stub.LineComment = "TODO: set the input that feeds this pipeline (e.g. your source topic)"
		input = component("stdin", stub)
	}
	kv(root, "input", input)

	if len(procs) > 0 {
		pipeline := mapping()
		kv(pipeline, "processors", seq(procs...))
		kv(root, "pipeline", pipeline)
	}

	output := comp.Output
	if output == nil {
		stub := mapping()
		stub.LineComment = "TODO: set the output destination"
		output = component("stdout", stub)
	}
	kv(root, "output", output)

	// Attach unmapped-field TODOs to the primary component's body node.
	primary := output
	if comp.Output == nil && comp.Input != nil {
		primary = input
	}
	// primary is always a component() node: a 2-element mapping {nameScalar, body}.
	// The body is Content[1]; we attach unmapped-field TODOs there.
	if len(unmapped) > 0 && len(primary.Content) == 2 {
		body := primary.Content[1] // the component body map
		var lines []string
		for _, k := range unmapped {
			lines = append(lines, fmt.Sprintf("TODO: unmapped field %s=%v", k, cfg.Props[k]))
		}
		if body.HeadComment != "" {
			body.HeadComment += "\n"
		}
		body.HeadComment += strings.Join(lines, "\n")
	}

	return root
}
