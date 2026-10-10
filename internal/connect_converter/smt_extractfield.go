// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package connectconverter

import "gopkg.in/yaml.v3"

func init() {
	registerSMT("org.apache.kafka.connect.transforms.ExtractField$Value", extractFieldSMT{})
	registerSMT("org.apache.kafka.connect.transforms.ExtractField$Key", extractFieldSMT{})
}

type extractFieldSMT struct{}

func (extractFieldSMT) Map(smt SMTConfig, ctx *MapCtx) ([]*yaml.Node, error) {
	field, _ := smt.Props["field"].(string)
	var expr *yaml.Node
	if field == "" {
		expr = scalar("root = this")
		expr.LineComment = "TODO: ExtractField without a field — map manually"
		ctx.Warn(smt.Alias, "ExtractField is missing the 'field' property; emitted a passthrough stub")
	} else {
		expr = scalar("root = " + fieldPath("this", field))
	}
	annotateKeyVariant(smt, expr, ctx)
	return []*yaml.Node{mappingProc(expr)}, nil
}
