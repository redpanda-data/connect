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

	"gopkg.in/yaml.v3"
)

func init() {
	registerSMT("org.apache.kafka.connect.transforms.HoistField$Value", hoistFieldSMT{})
	registerSMT("org.apache.kafka.connect.transforms.HoistField$Key", hoistFieldSMT{})
}

type hoistFieldSMT struct{}

func (hoistFieldSMT) Map(smt SMTConfig, ctx *MapCtx) ([]*yaml.Node, error) {
	field, _ := smt.Props["field"].(string)
	var expr *yaml.Node
	if field == "" {
		expr = scalar("root = this")
		expr.LineComment = "TODO: HoistField without a field — map manually"
		ctx.Warn(smt.Alias, "HoistField is missing the 'field' property; emitted a passthrough stub")
	} else {
		expr = scalar(fmt.Sprintf("root = {%q: this}", field))
	}
	annotateKeyVariant(smt, expr, ctx)
	return []*yaml.Node{mappingProc(expr)}, nil
}
