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
	registerSMT("org.apache.kafka.connect.transforms.Filter", filterSMT{})
}

type filterSMT struct{}

func (filterSMT) Map(smt SMTConfig, ctx *MapCtx) ([]*yaml.Node, error) {
	// Consume all predicates.* keys from the full config so they don't surface
	// as unmapped fields. We do this regardless of whether Filter uses them.
	consumeAllPredicateKeys(ctx)

	// Retrieve predicate name and negate flag from the SMT sub-props
	// (already stripped of "transforms.<alias>." prefix by mapSMTs).
	predName, _ := smt.Props["predicate"].(string)
	predName = strings.TrimSpace(predName)

	negateStr, _ := smt.Props["negate"].(string)
	negate := strings.EqualFold(strings.TrimSpace(negateStr), "true")

	// No predicate → drop ALL records.
	if predName == "" {
		s := scalar("root = deleted()")
		s.LineComment = "Filter with no predicate drops all records"
		return []*yaml.Node{mappingProc(s)}, nil
	}

	// Resolve the predicate expression.
	predExpr, ok := resolvePredicate(predName, ctx)
	if !ok {
		// Unknown predicate class → passthrough stub with TODO + warning.
		s := scalar("root = this")
		s.LineComment = "TODO: unsupported predicate for Filter — map manually"
		ctx.Warn(smt.Alias, fmt.Sprintf("unsupported predicate class for predicate %q — map manually", predName))
		return []*yaml.Node{mappingProc(s)}, nil
	}

	// Build the drop expression.
	var expr string
	if negate {
		expr = fmt.Sprintf("root = if !(%s) { deleted() }", predExpr)
	} else {
		expr = fmt.Sprintf("root = if %s { deleted() }", predExpr)
	}
	return []*yaml.Node{mappingProc(scalar(expr))}, nil
}

// consumeAllPredicateKeys consumes the top-level "predicates" key and all
// "predicates.<name>.*" keys from the full config context so they do not
// surface as unmapped fields.
func consumeAllPredicateKeys(ctx *MapCtx) {
	ctx.consume("predicates")
	for k := range ctx.cfg.Props {
		if strings.HasPrefix(k, "predicates.") {
			ctx.consume(k)
		}
	}
}

// resolvePredicate looks up the predicate class for predName and returns the
// Bloblang expression for that predicate. Returns ("", false) if the predicate
// name is not found in the config or the class is unsupported.
func resolvePredicate(predName string, ctx *MapCtx) (string, bool) {
	typeKey := "predicates." + predName + ".type"
	predClass, ok := ctx.Lookup(typeKey)
	if !ok {
		return "", false
	}

	switch predClass {
	case "org.apache.kafka.connect.transforms.predicates.RecordIsTombstone":
		return "this == null", true

	case "org.apache.kafka.connect.transforms.predicates.TopicNameMatches":
		patternKey := "predicates." + predName + ".pattern"
		pattern, _ := ctx.Lookup(patternKey)
		return fmt.Sprintf(`@kafka_topic.re_match(%q)`, pattern), true

	case "org.apache.kafka.connect.transforms.predicates.HasHeaderKey":
		nameKey := "predicates." + predName + ".name"
		headerName, _ := ctx.Lookup(nameKey)
		return fmt.Sprintf(`metadata(%q) != null`, headerName), true

	default:
		return "", false
	}
}
