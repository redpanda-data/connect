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
	registerSMT("org.apache.kafka.connect.transforms.InsertField$Value", insertFieldSMT{})
	registerSMT("org.apache.kafka.connect.transforms.InsertField$Key", insertFieldSMT{})
}

type insertFieldSMT struct{}

func (insertFieldSMT) Map(smt SMTConfig, _ *MapCtx) ([]*yaml.Node, error) {
	var lines []string

	// static.field + static.value
	if staticField, ok := smt.Props["static.field"].(string); ok && staticField != "" {
		var value string
		if raw, ok := smt.Props["static.value"]; ok {
			value = fmt.Sprint(raw)
		}
		lines = append(lines, fmt.Sprintf("root.%s = %q", staticField, value))
	}

	// timestamp.field — use kafka_timestamp_ms from the kafka/redpanda input metadata.
	if tsField, ok := smt.Props["timestamp.field"].(string); ok && tsField != "" {
		lines = append(lines, fmt.Sprintf(`root.%s = metadata("kafka_timestamp_ms")`, tsField))
	}

	// topic.field
	if topicField, ok := smt.Props["topic.field"].(string); ok && topicField != "" {
		lines = append(lines, fmt.Sprintf("root.%s = @kafka_topic", topicField))
	}

	// partition.field
	if partField, ok := smt.Props["partition.field"].(string); ok && partField != "" {
		lines = append(lines, fmt.Sprintf(`root.%s = metadata("kafka_partition")`, partField))
	}

	// offset.field
	if offsetField, ok := smt.Props["offset.field"].(string); ok && offsetField != "" {
		lines = append(lines, fmt.Sprintf(`root.%s = metadata("kafka_offset")`, offsetField))
	}

	var expr *yaml.Node
	if len(lines) > 0 {
		expr = scalar(strings.Join(lines, "\n"))
	} else {
		expr = scalar("root = this")
		expr.LineComment = "TODO: InsertField without recognised field properties — map manually"
	}
	return []*yaml.Node{mappingProc(expr)}, nil
}
