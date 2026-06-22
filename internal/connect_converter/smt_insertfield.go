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
	registerSMT("org.apache.kafka.connect.transforms.InsertField$Value", insertFieldSMT{})
	registerSMT("org.apache.kafka.connect.transforms.InsertField$Key", insertFieldSMT{})
}

type insertFieldSMT struct{}

func (insertFieldSMT) Map(smt SMTConfig, _ *MapCtx) ([]*yaml.Node, error) {
	field, _ := smt.Props["static.field"].(string)
	value, _ := smt.Props["static.value"].(string)
	var expr *yaml.Node
	if field != "" {
		expr = scalar(fmt.Sprintf("root.%s = %q", field, value))
	} else {
		expr = scalar("root = this")
		expr.LineComment = "TODO: InsertField without static.field — map manually (timestamp/topic/etc.)"
	}
	// The mapping processor takes its Bloblang directly as a string value.
	return []*yaml.Node{component("mapping", expr)}, nil
}
