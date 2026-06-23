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

func (replaceFieldSMT) Map(smt SMTConfig, _ *MapCtx) ([]*yaml.Node, error) {
	var lines []string
	found := false

	// renames: old:new,old2:new2
	if renames, ok := smt.Props["renames"].(string); ok && renames != "" {
		found = true
		for pair := range strings.SplitSeq(renames, ",") {
			kvp := strings.SplitN(strings.TrimSpace(pair), ":", 2)
			if len(kvp) == 2 {
				lines = append(lines, fmt.Sprintf("%s = %s", fieldPath("root", kvp[1]), fieldPath("this", kvp[0])))
				lines = append(lines, fieldPath("root", kvp[0])+" = deleted()")
			}
		}
	}

	// exclude / blacklist: field1,field2
	excludeVal, _ := smt.Props["exclude"].(string)
	blacklistVal, _ := smt.Props["blacklist"].(string)
	excluded := excludeVal
	if excluded == "" {
		excluded = blacklistVal
	}
	if excluded != "" {
		found = true
		for f := range strings.SplitSeq(excluded, ",") {
			lines = append(lines, fieldPath("root", strings.TrimSpace(f))+" = deleted()")
		}
	}

	// include / whitelist: field1,field2 — keep only listed fields.
	includeVal, _ := smt.Props["include"].(string)
	whitelistVal, _ := smt.Props["whitelist"].(string)
	included := includeVal
	if included == "" {
		included = whitelistVal
	}
	if included != "" {
		found = true
		// Emit projection: root = {} then root.<f> = this.<f> for each kept field.
		lines = append(lines, "root = {}")
		for f := range strings.SplitSeq(included, ",") {
			trimmed := strings.TrimSpace(f)
			if trimmed != "" {
				lines = append(lines, fmt.Sprintf("%s = %s", fieldPath("root", trimmed), fieldPath("this", trimmed)))
			}
		}
	}

	var expr *yaml.Node
	if !found {
		expr = scalar("root = this")
		expr.LineComment = "TODO: ReplaceField has no parseable properties (renames/exclude/blacklist/include/whitelist) — map manually"
	} else {
		expr = scalar(strings.Join(lines, "\n"))
	}
	return []*yaml.Node{mappingProc(expr)}, nil
}
