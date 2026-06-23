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
	registerSMT("org.apache.kafka.connect.transforms.ValueToKey", valueToKeySMT{})
}

type valueToKeySMT struct{}

func (valueToKeySMT) Map(smt SMTConfig, ctx *MapCtx) ([]*yaml.Node, error) {
	fields, _ := smt.Props["fields"].(string)

	var names []string
	for f := range strings.SplitSeq(fields, ",") {
		f = strings.TrimSpace(f)
		if f != "" {
			names = append(names, f)
		}
	}

	var expr *yaml.Node
	switch len(names) {
	case 0:
		expr = scalar("root = this")
		expr.LineComment = "TODO: ValueToKey without 'fields' — map manually"
		ctx.Warn(smt.Alias, "ValueToKey is missing the 'fields' property; emitted a passthrough stub")
	case 1:
		expr = scalar(fmt.Sprintf("meta key = this.%s.string()", names[0]))
	default:
		expr = scalar(fmt.Sprintf("meta key = this.%s.string()", names[0]))
		expr.LineComment = "TODO: ValueToKey lists multiple fields — combine them into the key manually"
		ctx.Warn(smt.Alias, "ValueToKey lists multiple fields; only the first was mapped — review and combine the remaining fields into the key")
	}
	return []*yaml.Node{mappingProc(expr)}, nil
}
