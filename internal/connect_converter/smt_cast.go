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
	registerSMT("org.apache.kafka.connect.transforms.Cast$Value", castSMT{})
	registerSMT("org.apache.kafka.connect.transforms.Cast$Key", castSMT{})
}

type castSMT struct{}

// castMethod maps a Kafka Connect Cast target type to a Bloblang coercion
// method. The bool reports whether the type is known.
func castMethod(typ string) (string, bool) {
	switch strings.TrimSpace(typ) {
	case "int8", "int16", "int32", "int64", "float32", "float64":
		return "number", true
	case "string":
		return "string", true
	case "boolean":
		return "bool", true
	default:
		return "", false
	}
}

func (castSMT) Map(smt SMTConfig, ctx *MapCtx) ([]*yaml.Node, error) {
	spec, _ := smt.Props["spec"].(string)

	var lines []string
	var hadUnknown bool
	for pair := range strings.SplitSeq(spec, ",") {
		kvp := strings.SplitN(strings.TrimSpace(pair), ":", 2)
		if len(kvp) != 2 {
			continue
		}
		field, typ := strings.TrimSpace(kvp[0]), strings.TrimSpace(kvp[1])
		if field == "" {
			continue
		}
		if method, ok := castMethod(typ); ok {
			lines = append(lines, fmt.Sprintf("root.%s = this.%s.%s()", field, field, method))
		} else {
			lines = append(lines, fmt.Sprintf("root.%s = this.%s # TODO: unknown cast type %s — map manually", field, field, typ))
			hadUnknown = true
		}
	}

	var expr *yaml.Node
	if len(lines) == 0 {
		expr = scalar("root = this")
		expr.LineComment = "TODO: Cast without a valid 'spec' — map manually"
		ctx.Warn(smt.Alias, "Cast is missing a parseable 'spec' property; emitted a passthrough stub")
	} else {
		expr = scalar(strings.Join(lines, "\n"))
		if hadUnknown {
			ctx.Warn(smt.Alias, "Cast spec contains an unrecognised target type; review the TODO-annotated field(s)")
		}
	}
	annotateKeyVariant(smt, expr, ctx)
	// The mapping processor takes its Bloblang directly as a string value.
	return []*yaml.Node{component("mapping", expr)}, nil
}
