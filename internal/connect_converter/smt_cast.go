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
	case "int8", "int16", "int32", "int64":
		return "int64", true
	case "float32", "float64":
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

	// Detect whole-value form: a single token with no colon.
	trimmedSpec := strings.TrimSpace(spec)
	if trimmedSpec != "" && !strings.Contains(trimmedSpec, ":") && !strings.Contains(trimmedSpec, ",") {
		// whole-value/whole-key cast
		var expr *yaml.Node
		if method, ok := castMethod(trimmedSpec); ok {
			expr = scalar(fmt.Sprintf("root = this.%s()", method))
		} else {
			expr = scalar("root = this")
			expr.LineComment = "TODO: Cast whole-value with unknown type " + trimmedSpec + " — map manually"
			ctx.Warn(smt.Alias, "Cast whole-value spec has unrecognised type "+trimmedSpec+"; emitted a passthrough stub")
		}
		annotateKeyVariant(smt, expr, ctx)
		return []*yaml.Node{mappingProc(expr)}, nil
	}

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
			lines = append(lines, fmt.Sprintf("%s = %s.%s()", fieldPath("root", field), fieldPath("this", field), method))
		} else {
			lines = append(lines, fmt.Sprintf("%s = %s # TODO: unknown cast type %s — map manually", fieldPath("root", field), fieldPath("this", field), typ))
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
	return []*yaml.Node{mappingProc(expr)}, nil
}
