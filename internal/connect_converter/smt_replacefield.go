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

func init() {
	registerSMT("org.apache.kafka.connect.transforms.ReplaceField$Value", replaceFieldSMT{})
	registerSMT("org.apache.kafka.connect.transforms.ReplaceField$Key", replaceFieldSMT{})
}

type replaceFieldSMT struct{}

func (replaceFieldSMT) Map(smt SMTConfig, ctx *MapCtx) ([]*yaml.Node, error) {
	var lines []string
	if renames, ok := smt.Props["renames"].(string); ok && renames != "" {
		for _, pair := range strings.Split(renames, ",") {
			kvp := strings.SplitN(strings.TrimSpace(pair), ":", 2)
			if len(kvp) == 2 {
				lines = append(lines, fmt.Sprintf("root.%s = this.%s", kvp[1], kvp[0]))
				lines = append(lines, fmt.Sprintf("root.%s = deleted()", kvp[0]))
			}
		}
	}
	if exclude, ok := smt.Props["exclude"].(string); ok && exclude != "" {
		for _, f := range strings.Split(exclude, ",") {
			lines = append(lines, fmt.Sprintf("root.%s = deleted()", strings.TrimSpace(f)))
		}
	}
	var expr *yaml.Node
	if len(lines) == 0 {
		expr = scalar("root = this")
		expr.LineComment = "TODO: ReplaceField with include/whitelist semantics — map manually"
	} else {
		expr = scalar(strings.Join(lines, "\n"))
	}
	// The mapping processor takes its Bloblang directly as a string value.
	return []*yaml.Node{component("mapping", expr)}, nil
}
