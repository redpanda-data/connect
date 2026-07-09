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
	registerSMT("org.apache.kafka.connect.transforms.MaskField$Value", maskFieldSMT{})
	registerSMT("org.apache.kafka.connect.transforms.MaskField$Key", maskFieldSMT{})
}

type maskFieldSMT struct{}

func (maskFieldSMT) Map(smt SMTConfig, ctx *MapCtx) ([]*yaml.Node, error) {
	fields, _ := smt.Props["fields"].(string)
	replacement, _ := smt.Props["replacement"].(string)

	var lines []string
	for f := range strings.SplitSeq(fields, ",") {
		f = strings.TrimSpace(f)
		if f == "" {
			continue
		}
		lines = append(lines, fmt.Sprintf("%s = %q", fieldPath("root", f), replacement))
	}

	var expr *yaml.Node
	if len(lines) == 0 {
		expr = scalar("root = this")
		expr.LineComment = "TODO: MaskField without 'fields' — map manually"
		ctx.Warn(smt.Alias, "MaskField is missing the 'fields' property; emitted a passthrough stub")
	} else {
		expr = scalar(strings.Join(lines, "\n"))
		expr.LineComment = "TODO: MaskField sets masked fields to an empty string; numeric/boolean fields will be stringified — review and adjust types if needed"
		ctx.Warn(smt.Alias, "MaskField sets masked fields to an empty string; numeric/boolean fields will be stringified — review and adjust types if needed")
	}
	annotateKeyVariant(smt, expr, ctx)
	return []*yaml.Node{mappingProc(expr)}, nil
}
