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
	registerSMT("org.apache.kafka.connect.transforms.TimestampConverter$Value", timestampConverterSMT{})
	registerSMT("org.apache.kafka.connect.transforms.TimestampConverter$Key", timestampConverterSMT{})
}

type timestampConverterSMT struct{}

func (timestampConverterSMT) Map(smt SMTConfig, ctx *MapCtx) ([]*yaml.Node, error) {
	field, _ := smt.Props["field"].(string)
	targetType, _ := smt.Props["target.type"].(string)

	var expr *yaml.Node
	switch {
	case field == "":
		expr = scalar("root = this")
		expr.LineComment = "TODO: TimestampConverter without a 'field' — map manually"
		ctx.Warn(smt.Alias, "TimestampConverter is missing the 'field' property; emitted a passthrough stub")
	case targetType == "unix":
		expr = scalar(fmt.Sprintf("root.%s = this.%s.ts_unix()", field, field))
	case targetType == "string":
		expr = scalar(fmt.Sprintf(`root.%s = this.%s.ts_format("2006-01-02T15:04:05Z07:00")`, field, field))
		expr.LineComment = "TODO: set the target format to match the SMT's 'format' property"
		ctx.Warn(smt.Alias, "TimestampConverter target.type=string: review the emitted ts_format layout against the SMT's 'format'")
	case targetType == "Timestamp" || targetType == "Date" || targetType == "Time":
		expr = scalar(fmt.Sprintf(`root.%s = this.%s.ts_parse("2006-01-02T15:04:05Z07:00")`, field, field))
		expr.LineComment = "TODO: set the input layout to parse target.type=" + targetType
		ctx.Warn(smt.Alias, "TimestampConverter target.type="+targetType+": review the emitted ts_parse layout against your timestamp format")
	default:
		expr = scalar("root = this")
		expr.LineComment = "TODO: TimestampConverter with unsupported target.type=" + targetType + " — map manually"
		ctx.Warn(smt.Alias, "TimestampConverter has an unrecognised target.type="+targetType+"; emitted a passthrough stub")
	}
	annotateKeyVariant(smt, expr, ctx)
	// The mapping processor takes its Bloblang directly as a string value.
	return []*yaml.Node{component("mapping", expr)}, nil
}
